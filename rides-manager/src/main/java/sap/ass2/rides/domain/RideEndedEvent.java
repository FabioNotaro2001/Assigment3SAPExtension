package sap.ass2.rides.domain;

public record RideEndedEvent(String rideID, String reason) implements RideEvent{
    public static RideEndedEvent from(String rideID, String reason){
        return new RideEndedEvent(rideID, reason);
    }

    @Override
    public String getRideId() {
        return this.rideID;
    }
}
