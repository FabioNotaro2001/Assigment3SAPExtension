package sap.ass2.rides.infrastructure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import sap.ass2.rides.application.Environment;
import io.vertx.core.json.JsonObject;
import sap.ass2.rides.application.RideEventParser;
import sap.ass2.rides.domain.EbikeEvent;
import sap.ass2.rides.domain.EbikeState;
import sap.ass2.rides.domain.RideState;
import sap.ass2.rides.domain.RideEndedEvent;
import sap.ass2.rides.domain.RideStartedEvent;
import sap.ass2.rides.domain.RideStepEvent;
import sap.ass2.rides.domain.UserEvent;
import sap.ass2.rides.domain.V2d;

public class RidesExecutionVerticle extends AbstractVerticle {
    // Events that this verticle can publish.
    private static String RIDES_STEP = "rides-step";
    private static String RIDE_STOP = "ride-stop";
    private boolean doLoop = false;

    // The key is rideID for both.
    Map<String, MessageConsumer<String>> rides; // Consumer on event loop.
    Map<String, TimeVariables> timeVars;    // Temporal variables for the ride (for example last credit decreased), useful to decide when the verticle has to decrease credit or battery level.
    Map<String, RideStopReason> stopRideRequested;
    Map<String, RideState> rideStates;

    private MessageConsumer<Object> loopConsumer;   // Consumer thats periodically sends events to the rides to make them proceed.

    static Logger logger = Logger.getLogger("[Rides Executor Verticle]");

    private Environment environment;
    private KafkaProducer<String, String> producer;
    
    public RidesExecutionVerticle(KafkaProducer<String, String> producer, Environment environment) {
        this.rides = new ConcurrentHashMap<>();
        this.timeVars = new ConcurrentHashMap<>();
        this.stopRideRequested = new ConcurrentHashMap<>();
        this.loopConsumer = null;
        this.rideStates = new ConcurrentHashMap<>();
        this.environment = environment;
        this.producer = producer;
    }

    public void launch() {
        Vertx vertx;
        if (Vertx.currentContext() != null) {
			vertx = Vertx.currentContext().owner();
		} else {
			vertx = Vertx.vertx();
		}

		vertx.deployVerticle(this);
	}

    public void start() {
        // Consumer that handles the stopping ride request (called from outside).
        this.vertx.eventBus().<String>consumer(RIDE_STOP, pair -> {
            var args = pair.body().split(" ");
            this.stopRideRequested.put(args[0], RideStopReason.valueOf(args[1]));   // Add request to the map containing stop reasons.
        });
    
        var eventBus = this.vertx.eventBus();

        this.loopConsumer = eventBus.consumer(RIDES_STEP);
        this.loopConsumer.handler(msg -> {
            if(!this.doLoop){
                return;
            }
            this.vertx.executeBlocking(() -> {  // executeBlocking to execute the sleep in a different thread, so that we don't stop the main thread (in this time the rides must complete their step).
                Thread.sleep(500);
                return null;
            }).onComplete(h -> {
                if (!rides.isEmpty()) {
                    eventBus.publish(RIDES_STEP, null); // Ask for the next step (to himself).
                    logger.log(Level.INFO, "LOOP STEP");
                } else {
                    this.doLoop = false;
                    logger.log(Level.INFO, "Loop paused...");
                }
            });
        });
    }
    
    private static JsonObject userEventToJSON(UserEvent event){
        return new JsonObject()
            .put("userId", event.userId())
            .put("credits", event.creditDelta())
            .put("deltaX", event.deltaX())
            .put("deltaY", event.deltaY());
    }

    private static JsonObject ebikeEventToJSON(EbikeEvent event){
        return new JsonObject()
            .put("ebikeId", event.ebikeId())
            .put("newState", event.newState().orElse(null))
            .put("deltaPosX", event.deltaPos().x())
            .put("deltaPosY", event.deltaPos().y())
            .put("deltaDirX", event.deltaDir().x())
            .put("deltaDirY", event.deltaDir().y())
            .put("deltaSpeed", event.deltaSpeed())
            .put("deltaBatteryLevel", event.deltaBatteryLevel());
    }

    private void sendRideEvent(RideStartedEvent event){
        this.producer.send(new ProducerRecord<String,String>("ride-events", RideEventParser.toJSON(event).encode()));
        logger.log(Level.INFO, "Inviato evento ride start");
    }

    private void sendRideEvent(RideStepEvent event){
        this.producer.send(new ProducerRecord<String,String>("ride-events", RideEventParser.toJSON(event).encode()));
        logger.log(Level.INFO, "Inviato ride step");
    }
    
    private void sendRideEvent(RideEndedEvent event){
        this.producer.send(new ProducerRecord<String,String>("ride-events", RideEventParser.toJSON(event).encode()));
        logger.log(Level.INFO, "Inviato evento ride end");
    }

    private void sendUserEvent(UserEvent event){
        this.producer.send(new ProducerRecord<String,String>("user-events", userEventToJSON(event).encode()));
    }

    private void sendEbikeEvent(EbikeEvent event){
        this.producer.send(new ProducerRecord<String,String>("ebike-events", ebikeEventToJSON(event).encode()));
    }

    private void beginLoopOfEventsIfNecessary() {
        if (this.doLoop) {
            return;
        }
        this.doLoop = true;
        logger.log(Level.INFO, "Resuming loop...");
        this.vertx.eventBus().publish(RIDES_STEP, null);
    }

    private List<Double> rotate(double x, double y, double degrees) {
        var rad = degrees * Math.PI / 180; // Converts degrees to radians.
        var cs = Math.cos(rad); // Calculates the cosine of the angle.
        var sn = Math.sin(rad); // Calculates the sine of the angle.
        var xN = x * cs - y * sn; // New x component after rotation.
        var yN = x * sn + y * cs; // New y component after rotation.
        var module = (double) Math.sqrt(xN * xN + yN * yN);
        return List.of(xN / module, yN / module); // Returns the rotated and normalized vector.
    }

    private void clearRide(String rideID){
        this.rides.remove(rideID);
        this.rideStates.remove(rideID);
        this.timeVars.remove(rideID);
        this.stopRideRequested.remove(rideID);
    }

    // Called by RidesManager.
    public void launchRide(String rideID, String userID, String ebikeID) {
        this.timeVars.put(rideID, TimeVariables.now());
        this.rideStates.put(rideID, RideState.GOING_TO_USER);

        var eventBus = this.vertx.eventBus();

        this.sendEbikeEvent(EbikeEvent.from(ebikeID, Optional.ofNullable(EbikeState.IN_USE), new V2d(0, 0), new V2d(0, 0), 1, 0));

        MessageConsumer<String> consumer = eventBus.<String>consumer(RIDES_STEP);
        consumer.handler(msg -> {

            // SENSE PHASE.
            var user = this.environment.getUser(userID);
            var ebike = this.environment.getEbike(ebikeID);
            // END OF SENSE PHASE.  

            if(user == null || ebike == null){
                logger.log(Level.INFO, "qualcosa è null!!!!!!!!!!!!!");
                this.clearRide(rideID);
                this.sendRideEvent(RideEndedEvent.from(rideID, RideStopReason.SERVICE_ERROR.reason));
                this.sendEbikeEvent(EbikeEvent.from(ebikeID, Optional.ofNullable(EbikeState.AVAILABLE), new V2d(0, 0), new V2d(0, 0), -ebike.speed(), 0));
                consumer.unregister();
                return;
            }
            
            // Checks of the ride must be stopped.
            var stopRequestedOpt = Optional.ofNullable(this.stopRideRequested.get(rideID));
            if (stopRequestedOpt.isPresent()) {
                this.clearRide(rideID);
                this.sendRideEvent(RideEndedEvent.from(rideID, stopRequestedOpt.get().reason));
                this.sendEbikeEvent(EbikeEvent.from(ebikeID, Optional.ofNullable(ebike.batteryLevel() > 0 ? EbikeState.AVAILABLE : EbikeState.MAINTENANCE), new V2d(0, 0), new V2d(0, 0), -ebike.speed(), 0));
                consumer.unregister();
                
                logger.log(Level.INFO, "Ride " + rideID + " stopped");
                return;
            }
            
            TimeVariables timeVar = this.timeVars.get(rideID);

            // DECIDE PHASE.
            RideState rideState = this.rideStates.get(rideID);
            if(rideState == RideState.GOING_TO_USER){
                logger.log(Level.INFO, "stato ride going to user!!!!!!!!!");
                var oldX = ebike.locX();
                var oldY = ebike.locY();
                var dirX = user.x() - oldX;
                var dirY = user.y() - oldY;
                var dist = Math.sqrt(dirX * dirX + dirY * dirY);
                var newX = oldX; 
                var newY = oldY;

                if(dist != 0){
                    newX = oldX + dirX / dist;
                    newY = oldY + dirY / dist;
                }

                var newDirX = user.x() - newX;
                var newDirY = user.y() - newY;
                var newDist = Math.sqrt(newDirX * newDirX + newDirY * newDirY);
                boolean userReached = false;
                if(newDist >= dist){
                    newX = user.x();
                    newY = user.y();
                    userReached = true;
                    this.rideStates.put(rideID, RideState.REGULAR);
                }
                // END OF DECIDE PHASE.

                // ACT PHASE.
                // Notify observer about the current ride status.
                this.sendRideEvent(RideStepEvent.from(rideID, newX, newY, dirX, dirY, 1, ebike.batteryLevel()));
                this.sendEbikeEvent(EbikeEvent.from(ebikeID, Optional.empty(), new V2d(newX - oldX, newY - oldY), 
                    new V2d((!userReached ? dirX / dist : 1.0) - ebike.dirX(), (!userReached ? dirY / dist : 0.0) - ebike.dirY()), 0, 0));                    
                // END OF ACT PHASE.
            } else {    // Regular ride type.
                // DECIDE PHASE.
                logger.log(Level.INFO, "stato ride normale!!!!!!!!");
                if (ebike.batteryLevel() > 0 && user.credit() > 0) {
                    // Get current location and direction of the bike.
                    var oldX = ebike.locX();
                    var oldY = ebike.locY();
                    var dirX = ebike.dirX();
                    var dirY = ebike.dirY();
                    var s = 1; // Speed of the bike
                    
                    // Calculate new location based on direction.
                    var newX = oldX + dirX * s;
                    var newY = oldY + dirY * s;
    
                    var newDirX = dirX;
                    var newDirY = dirY;
                    
                    var batteryLevelDecrease = 0; // Battery level.
                    
                    // Handle boundary conditions for bike's location.
                    if (newX > 200 || newX < -200) {
                        newDirX = -newDirX;
                        newX = newX > 200 ? 200 : -200;
                    }
                    if (newY > 200 || newY < -200) {
                        newDirY = -newDirY;
                        newY = newY > 200 ? 200 : -200;
                    }
                    
                    // Change direction randomly every 500 milliseconds.
                    var elapsedTimeSinceLastChangeDir = System.currentTimeMillis() - timeVar.lastTimeChangedDir();
                    if (elapsedTimeSinceLastChangeDir > 500) {
                        double angle = Math.random() * 60 - 30; // Random angle between -30 and 30 degrees.
                        var newDir = rotate(dirX, dirY, angle);
                        newDirX = newDir.get(0);
                        newDirY = newDir.get(1);
                        
                        timeVar = timeVar.updateLastTimeChangedDir(System.currentTimeMillis());
                    }
                    //END OF DECIDE PHASE.
                    
                    // ACT PHASE.
                    // Update user credits every 2000 milliseconds.
                    var elapsedTimeSinceLastDecredit = System.currentTimeMillis() - timeVar.lastTimeDecreasedCredit();
                    var userCreditDecrease = 0;
                    if (elapsedTimeSinceLastDecredit > 2000) {
                        userCreditDecrease--;
                        
                        timeVar = timeVar.updateLastTimeDecreasedCredit(System.currentTimeMillis());
                    }
                    
                    // Decrease battery level every 1500 milliseconds.
                    var elapsedTimeSinceLastBatteryDecreased = System.currentTimeMillis() - timeVar.lastTimeBatteryDecreased();
                    if (elapsedTimeSinceLastBatteryDecreased > 1500) {
                        batteryLevelDecrease--;
                        
                        timeVar = timeVar.updateLastTimeBatteryDecreased(System.currentTimeMillis());
                    }
                    
                    // Notify observer about the current ride status.
                    this.sendRideEvent(RideStepEvent.from(rideID, newX, newY, newDirX, newDirY, 1, ebike.batteryLevel() + batteryLevelDecrease));
                    this.sendEbikeEvent(EbikeEvent.from(ebikeID, Optional.empty(), new V2d(newX - oldX, newY - oldY), new V2d(newDirX - dirX, newDirY - dirY), 0, batteryLevelDecrease));
                    this.sendUserEvent(UserEvent.from(userID, userCreditDecrease, newX - user.x(), newY - user.y()));
                    // END OF ACT PHASE.
                    
                    this.timeVars.put(rideID, timeVar);
                    
                    logger.log(Level.INFO, "Ride " + rideID + " event");
                } else {
                    // The ride cannot proceed (insufficient credit or battery level).
                    this.sendEbikeEvent(EbikeEvent.from(ebikeID, Optional.of(ebike.batteryLevel() > 0 ? EbikeState.AVAILABLE : EbikeState.MAINTENANCE), new V2d(0, 0), new V2d(0, 0), -ebike.speed(), 0));
                    
                    this.clearRide(rideID);
                    this.sendRideEvent(RideEndedEvent.from(rideID, (ebike.batteryLevel() > 0 ? RideStopReason.USER_RAN_OUT_OF_CREDIT : RideStopReason.EBIKE_RAN_OUT_OF_BATTERY).reason));
                    consumer.unregister();
                    
                    logger.log(Level.INFO, "Ride " + rideID + " ended");
                }
            }
        });

        this.rides.put(rideID, consumer);
        this.sendRideEvent(RideStartedEvent.from(rideID, userID, ebikeID));
        this.beginLoopOfEventsIfNecessary();
    }

    public void stopRide(String rideID) {
        if (this.rides.containsKey(rideID)) {
            this.vertx.eventBus().publish(RIDE_STOP, rideID + " " + RideStopReason.RIDE_STOPPED.toString());
        }
    }
}
