package sap.ass2.rides.domain;

import sap.ddd.Entity;

public record Ebike(String id, EbikeState state, double locX, double locY, double dirX, double dirY, double speed, int batteryLevel) implements Entity<String> {
    @Override
    public String getId() {
        return id;
    }

    public Ebike applyEvent(EbikeEvent event){
        if (this.id.equals(event.ebikeId())) {
            var state = this.state;
            if (event.newState().isPresent()) {
                state = event.newState().get();
            }
            return new Ebike(id, state, locX + event.deltaPos().x(), locY + event.deltaPos().y(), dirX + event.deltaDir().x(), dirY + event.deltaDir().y(), speed + event.deltaSpeed(), batteryLevel + event.deltaBatteryLevel());
        }
        return this;
    }

    public static Ebike from(EbikeEvent event){
        return new Ebike(event.ebikeId(), event.newState().orElse(EbikeState.AVAILABLE), event.deltaPos().x(), event.deltaPos().y(), event.deltaDir().x(), event.deltaDir().y(), event.deltaSpeed(), event.deltaBatteryLevel());
    }
}