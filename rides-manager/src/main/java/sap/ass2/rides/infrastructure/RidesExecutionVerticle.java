package sap.ass2.rides.infrastructure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import sap.ass2.rides.application.EbikesManagerRemoteAPI;
import sap.ass2.rides.application.Environment;
import sap.ass2.rides.application.UsersManagerRemoteAPI;
import sap.ass2.rides.domain.EbikeState;
import sap.ass2.rides.domain.RideEventObserver;
import sap.ass2.rides.domain.RideState;

public class RidesExecutionVerticle extends AbstractVerticle {
    // Events that this verticle can publish.
    private static String RIDES_STEP = "rides-step";
    private static String RIDE_STOP = "ride-stop";

    private RideEventObserver observer;
    private EbikesManagerRemoteAPI ebikesManager;   // Ebikes service.
    private UsersManagerRemoteAPI usersManager; // Users service.
    private boolean doLoop = false;

    // The key is rideID for both.
    Map<String, MessageConsumer<String>> rides; // Consumer on event loop.
    Map<String, TimeVariables> timeVars;    // Temporal variables for the ride (for example last credit decreased), useful to decide when the verticle has to decrease credit or battery level.
    Map<String, RideStopReason> stopRideRequested;
    Map<String, RideState> rideStates;

    private MessageConsumer<Object> loopConsumer;   // Consumer thats periodically sends events to the rides to make them proceed.

    static Logger logger = Logger.getLogger("[Rides Executor Verticle]");	

    private Environment environment;

    public RidesExecutionVerticle(RideEventObserver observer, UsersManagerRemoteAPI usersManager, EbikesManagerRemoteAPI ebikesManager, Environment environment) {
        this.observer = observer;
        this.usersManager = usersManager;
        this.ebikesManager = ebikesManager;

        this.rides = new ConcurrentHashMap<>();
        this.timeVars = new ConcurrentHashMap<>();
        this.stopRideRequested = new ConcurrentHashMap<>();

        this.loopConsumer = null;
        this.rideStates = new ConcurrentHashMap<>();
        this.environment = environment;
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

        MessageConsumer<String> consumer = eventBus.<String>consumer(RIDES_STEP);
        consumer.handler(msg -> {

            // SENSE PHASE.
            var user = this.environment.getUser(userID);
            var ebike = this.environment.getEbike(ebikeID);
            // END OF SENSE PHASE.

            if(user == null || ebike == null){
                logger.log(Level.INFO, "qualcosa è null!!!!!!!!!!!!!");
                this.clearRide(rideID);
                this.observer.rideEnded(rideID, RideStopReason.SERVICE_ERROR.reason);
                this.ebikesManager.updateBike(ebikeID, Optional.ofNullable(EbikeState.AVAILABLE), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
                consumer.unregister();
                return;
            }

            // Checks of the ride must be stopped.
            var stopRequestedOpt = Optional.ofNullable(this.stopRideRequested.get(rideID));
            if (stopRequestedOpt.isPresent()) {
                this.clearRide(rideID);
                this.observer.rideEnded(rideID, stopRequestedOpt.get().reason);

                this.ebikesManager.updateBike(ebikeID, Optional.ofNullable(ebike.batteryLevel() > 0 ? EbikeState.AVAILABLE : EbikeState.MAINTENANCE), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
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
                if(newDist >= dist){
                    newX = user.x();
                    newY = user.y();
                    this.rideStates.put(rideID, RideState.REGULAR);
                }
                // END OF DECIDE PHASE.

                // ACT PHASE.
                // Notify observer about the current ride status.
                this.observer.rideStep(rideID, newX, newY, dirX, dirY, 1, ebike.batteryLevel());
                    
                this.ebikesManager.updateBike(ebikeID, Optional.empty(),
                Optional.of(newX), Optional.of(newY),
                Optional.of(dirX), Optional.of(dirY),
                Optional.of(1.0), Optional.empty());
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
                    
                    var newBatteryLevel = ebike.batteryLevel(); // Battery level.
                    
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
                    if (elapsedTimeSinceLastDecredit > 2000) {
                        usersManager.decreaseCredit(userID, 1);
                        
                        timeVar = timeVar.updateLastTimeDecreasedCredit(System.currentTimeMillis());
                    }
                    
                    // Decrease battery level every 1500 milliseconds.
                    var elapsedTimeSinceLastBatteryDecreased = System.currentTimeMillis() - timeVar.lastTimeBatteryDecreased();
                    if (elapsedTimeSinceLastBatteryDecreased > 1500) {
                        newBatteryLevel--;
                        
                        timeVar = timeVar.updateLastTimeBatteryDecreased(System.currentTimeMillis());
                    }
                    
                    // Notify observer about the current ride status.
                    this.observer.rideStep(rideID, newX, newY, newDirX, newDirY, 1, newBatteryLevel);
                    
                    this.ebikesManager.updateBike(ebikeID, Optional.empty(),
                    Optional.of(newX), Optional.of(newY),
                    Optional.of(newDirX), Optional.of(newDirY),
                    Optional.of(1.0), Optional.of(newBatteryLevel));
                    this.usersManager.move(userID, newX, newY);
                    // END OF ACT PHASE.
                    
                    this.timeVars.put(rideID, timeVar);
                    
                    logger.log(Level.INFO, "Ride " + rideID + " event");
                } else {
                    // The ride cannot proceed (insufficient credit or battery level).
                    this.ebikesManager.updateBike(ebikeID, Optional.of(ebike.batteryLevel() > 0 ? EbikeState.AVAILABLE : EbikeState.MAINTENANCE),
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
                    
                    this.clearRide(rideID);
                    this.observer.rideEnded(rideID, (ebike.batteryLevel() > 0 ? RideStopReason.USER_RAN_OUT_OF_CREDIT : RideStopReason.EBIKE_RAN_OUT_OF_BATTERY).reason);
                    consumer.unregister();
                    
                    logger.log(Level.INFO, "Ride " + rideID + " ended");
                }
            }
        });

        this.rides.put(rideID, consumer);
        this.observer.rideStarted(rideID, userID, ebikeID);
        this.beginLoopOfEventsIfNecessary();
    }

    public void stopRide(String rideID) {
        if (this.rides.containsKey(rideID)) {
            this.vertx.eventBus().publish(RIDE_STOP, rideID + " " + RideStopReason.RIDE_STOPPED.toString());
        }
    }
}
