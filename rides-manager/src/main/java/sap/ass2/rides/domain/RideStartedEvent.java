package sap.ass2.rides.domain;

public record RideStartedEvent(String rideID, String userID, String bikeID) implements RideEvent{
    public static RideStartedEvent from(String rideID, String userID, String bikeID){
        return new RideStartedEvent(rideID, userID, bikeID);
    }

    @Override
    public String getRideId() {
        return this.rideID;
    }
}
