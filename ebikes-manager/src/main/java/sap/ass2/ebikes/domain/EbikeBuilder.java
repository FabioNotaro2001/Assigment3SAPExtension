package sap.ass2.ebikes.domain;

public class EbikeBuilder {
    private String ebikeId = "";
    private Ebike.EbikeState state = Ebike.EbikeState.AVAILABLE;
    private P2d loc = P2d.zero();
    private V2d direction = V2d.zero();
    private double speed = 0;
    private int batteryLevel = 0;
    
    public EbikeBuilder() {
    
    }

    public void applyEvent(EbikeEvent event) {
        if (ebikeId.equals("")) {
            this.ebikeId = event.ebikeId();
        } else if (!ebikeId.equals(event.ebikeId())) {
            return;
        }
        
        if (event.newState().isPresent()) {
            this.state = event.newState().get();
        }

        this.loc = this.loc.sum(event.deltaPos());
        this.direction = this.direction.sum(event.deltaDir());
        this.speed += event.deltaSpeed();
        this.batteryLevel += event.deltaBatteryLevel();
    }

    public Ebike build() {
        return new Ebike(ebikeId, this.state, loc, direction, speed, batteryLevel);
    }
}