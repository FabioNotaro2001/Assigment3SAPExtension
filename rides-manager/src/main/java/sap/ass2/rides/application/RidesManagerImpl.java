package sap.ass2.rides.application;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.KafkaProducer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.rides.domain.EbikeState;
import sap.ass2.rides.domain.Ride;
import sap.ass2.rides.domain.RideEndedEvent;
import sap.ass2.rides.domain.RideEventConsumer;
import sap.ass2.rides.infrastructure.RidesExecutionVerticle;

public class RidesManagerImpl implements RidesManagerAPI, RideEventConsumer {
    private Map<String, Ride> rides;   // Ongoing rides.
    private int nextRideId;
    private RidesExecutionVerticle rideExecutor;    // Verticle that manages and executes the rides.
    private Environment environment;


    public RidesManagerImpl(Environment environment, KafkaProducer<String, String> producer, CustomKafkaListener listener){
        this.environment = environment;
        
        this.rides = new ConcurrentHashMap<>();
        this.nextRideId = 0;

        this.rideExecutor = new RidesExecutionVerticle(producer, this.environment);
        this.rideExecutor.launch();

        listener.onEach("ride-events", this::consumeEvents);
    }

    // Converts a ride to a JSON.
    private static JsonObject toJSON(Ride ride) {
        return new JsonObject()
            .put("rideId", ride.getId())
            .put("userId", ride.getUser().id())
            .put("ebikeId", ride.getEbike().id());
    }

    @Override
    public JsonArray getAllRides() {
        // succededFuture because we decided that every method should return a Future for consistency, so somewhere there is a succeededFuture to simulate a Future where normally it would not be needed.
        return this.rides.values().stream().map(RidesManagerImpl::toJSON).collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
    }

    @Override
    public JsonObject beginRide(String userID, String ebikeID) throws IllegalArgumentException {
        // Checks if there is already an ongoing ride with the required bike.
        if (rides.values().stream().anyMatch(r -> r.getEbike().id().equals(ebikeID))) {
            throw new IllegalArgumentException("Ebike " + ebikeID + " already in use!");
        }

        var user = Optional.ofNullable(this.environment.getUser(userID));
        var bike = Optional.ofNullable(this.environment.getEbike(ebikeID));
        if(user.isEmpty()){
            throw new IllegalArgumentException("User " + userID + " doesn't exist!");
        }
        if(bike.isEmpty()){
            throw new IllegalArgumentException("Ebike " + ebikeID + " doesn't exist!");
        }
        if(bike.get().state() != EbikeState.AVAILABLE){
            throw new IllegalArgumentException("Ebike " + ebikeID + " is not available!");
        }

        Ride newRide = new Ride(String.valueOf(this.nextRideId), user.get(), bike.get());
        this.rides.put(newRide.getId(), newRide);
        this.nextRideId++;
        
        // Starting ride verticle.
        this.rideExecutor.launchRide(newRide.getId(), newRide.getUser().id(), newRide.getEbike().id());
        return toJSON(newRide);
    }

    @Override
    public void stopRide(String rideID, String userID) throws IllegalArgumentException {
        var ride = this.rides.values().stream().filter(r -> r.getId().equals(rideID)).findFirst();
        if(ride.isEmpty()){
            throw new IllegalArgumentException("Ride not found!");
        }
        if(!ride.get().getUser().id().equals(userID)){
            throw new IllegalArgumentException("The current user cannot stop the specified ride!");
        }
        
        this.rideExecutor.stopRide(rideID); 
    }

    @Override
    public Optional<JsonObject> getRideByRideID(String rideID) {
        // succededFuture because we decided that every method should return a Future for consistency, so somewhere there is a succeededFuture to simulate a Future where normally it would not be needed.
        return this.rides.values().stream().filter(r -> r.getId().equals(rideID)).findFirst().map(RidesManagerImpl::toJSON);
    }

    @Override
    public Optional<JsonObject> getRideByEbikeID(String bikeID) {
        // succededFuture because we decided that every method should return a Future for consistency, so somewhere there is a succeededFuture to simulate a Future where normally it would not be needed.
        return this.rides.values().stream().filter(r -> r.getEbike().id().equals(bikeID)).findFirst().map(RidesManagerImpl::toJSON);
    }

    @Override
    public Optional<JsonObject> getRideByUserID(String userID) {
        // succededFuture because we decided that every method should return a Future for consistency, so somewhere there is a succeededFuture to simulate a Future where normally it would not be needed.
        return this.rides.values().stream().filter(r -> r.getUser().id().equals(userID)).findFirst().map(RidesManagerImpl::toJSON);
    }

    @Override
    public void consumeEvents(String message) {
        var event = RideEventParser.from(new JsonObject(message));
        if(event instanceof RideEndedEvent)  {
            this.rides.remove(event.getRideId());
        }
    }
}