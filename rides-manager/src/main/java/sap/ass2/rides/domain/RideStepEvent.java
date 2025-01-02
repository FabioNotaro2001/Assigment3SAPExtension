package sap.ass2.rides.domain;

public record RideStepEvent(String rideID, double x, double y, double directionX, double directionY, double speed, int batteryLevel) implements RideEvent{
    public static RideStepEvent from(String rideID, double x, double y, double directionX, double directionY, double speed, int batteryLevel){
        return new RideStepEvent(rideID, x, y, directionX, directionY, speed, batteryLevel);
    }

    @Override
    public String getRideId() {
        return this.rideID;
    }
}
