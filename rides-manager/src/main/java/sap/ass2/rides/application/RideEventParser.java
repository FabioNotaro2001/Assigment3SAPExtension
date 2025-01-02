package sap.ass2.rides.application;

import io.vertx.core.json.JsonObject;
import sap.ass2.rides.domain.RideEndedEvent;
import sap.ass2.rides.domain.RideEvent;
import sap.ass2.rides.domain.RideStartedEvent;
import sap.ass2.rides.domain.RideStepEvent;

public class RideEventParser {
    private RideEventParser(){}

    public static RideEvent from(JsonObject obj){
        switch (obj.getString("event")) {
            case "ride-start":
                return new RideStartedEvent(obj.getString("rideId"), obj.getString("userId"), obj.getString("bikeId"));
            case "ride-step":
                return new RideStepEvent(obj.getString("rideId"), obj.getDouble("x"), obj.getDouble("y"), obj.getDouble("dirX"), obj.getDouble("dirY"), obj.getDouble("speed"), obj.getInteger("batteryLevel"));
            case "ride-end":
                return new RideEndedEvent(obj.getString("rideId"), obj.getString("reason"));
            default:
                throw new IllegalArgumentException("Invalid event type: " + obj.getString("event"));
        }
    } 

    public static JsonObject toJSON(RideStartedEvent event){
        return new JsonObject()
            .put("event", "ride-start")
            .put("rideId", event.getRideId())
            .put("userId", event.userID())
            .put("bikeId", event.bikeID());
    }

    public static JsonObject toJSON(RideStepEvent event){
        return new JsonObject()
            .put("event", "ride-step")
            .put("rideId", event.getRideId())
            .put("x", event.x())
            .put("y", event.y())
            .put("dirX", event.directionX())
            .put("dirY", event.directionY())
            .put("speed", event.speed())
            .put("batteryLevel", event.batteryLevel());
    }

    public static JsonObject toJSON(RideEndedEvent event){
        return new JsonObject()
            .put("event", "ride-end")
            .put("rideId", event.getRideId())
            .put("reason", event.reason());
    }
}
