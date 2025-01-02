package sap.ass2.rides.application;

import java.util.Optional;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Java interface implemented by RidesManagerImpl.
 */
public interface RidesManagerAPI {
    JsonArray getAllRides();
    JsonObject beginRide(String userID, String ebikeID) throws IllegalArgumentException;
    void stopRide(String rideID, String userID) throws IllegalArgumentException;
    Optional<JsonObject> getRideByRideID(String rideID);
    Optional<JsonObject> getRideByEbikeID(String ebikeID);
    Optional<JsonObject> getRideByUserID(String userID);
}