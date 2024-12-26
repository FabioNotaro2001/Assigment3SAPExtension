package sap.ass2.rides.domain;

public record UserEvent(String userId, int creditDelta, double deltaX, double deltaY) {
    public static UserEvent from(String userId, int creditDelta, double deltaX, double deltaY) {
        return new UserEvent(userId, creditDelta, deltaX, deltaY);
    }
}