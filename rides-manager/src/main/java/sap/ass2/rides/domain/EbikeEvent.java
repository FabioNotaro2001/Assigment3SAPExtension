package sap.ass2.rides.domain;

import java.util.Optional;

public record EbikeEvent(String ebikeId, Optional<EbikeState> newState, V2d deltaPos, V2d deltaDir, double deltaSpeed, int deltaBatteryLevel) {
    public static EbikeEvent from(String ebikeId, Optional<EbikeState> newState, V2d deltaPos, V2d deltaDir, double deltaSpeed, int deltaBatteryLevel) {
        return new EbikeEvent(ebikeId, newState, deltaPos, deltaDir, deltaSpeed, deltaBatteryLevel);
    }
}