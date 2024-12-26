package sap.ass2.rides.application;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.rides.domain.Ebike;
import sap.ass2.rides.domain.EbikeEvent;
import sap.ass2.rides.domain.EbikeState;
import sap.ass2.rides.domain.User;
import sap.ass2.rides.domain.UserEvent;
import sap.ass2.rides.domain.V2d;

public class Environment {
    private Map<String, User> users;
    private Map<String, Ebike> ebikes;
    private boolean ready;

    public Environment(CustomKafkaListener listenerForUsers, CustomKafkaListener listenerForEbikes){
        listenerForUsers.onEach(this::consumeUsersEvent);
        listenerForEbikes.onEach(this::consumeEbikesEvent);
        this.ready = false;
    }

    private User userFromJSON(JsonObject obj){
        return new User(obj.getString("userId"), obj.getInteger("credit"), obj.getDouble("deltaX"), obj.getDouble("deltaY"));
    }

    private Ebike ebikeFromJSON(JsonObject obj){
        String id = obj.getString("ebikeId");
        EbikeState state = EbikeState.valueOf(obj.getString("state"));
        double x = obj.getDouble("x");
        double y = obj.getDouble("y");
        double directionX = obj.getDouble("dirX");
        double directionY = obj.getDouble("dirY");
        double speed = obj.getDouble("speed");
        int batteryLevel = obj.getInteger("batteryLevel");
        return new Ebike(id, state, x, y, directionX, directionY, speed, batteryLevel);
    }

    public void init(JsonArray users, JsonArray ebikes){
        this.users = users.stream().map(u -> userFromJSON((JsonObject)u)).collect(Collectors.toConcurrentMap(u -> u.id(), Function.identity()));
        this.ebikes = ebikes.stream().map(e -> ebikeFromJSON((JsonObject)e)).collect(Collectors.toConcurrentMap(e -> e.id(), Function.identity()));
        this.ready = true;
    }

    public User getUser(String id){
        return this.ready ? this.users.get(id) : null;
    }

    public Ebike getEbike(String id){
        return this.ready ? this.ebikes.get(id) : null;
    }

    public boolean isReady(){
        return this.ready;
    }

    private void consumeUsersEvent(String message){
        JsonObject obj = new JsonObject(message);
        var event = UserEvent.from(obj.getString("userId"), obj.getInteger("credits"), obj.getDouble("deltaX"), obj.getDouble("deltaY"));
        String id = event.userId();
        if(this.users.containsKey(id)){
            var user = this.users.get(id);
            this.users.put(id, user.applyEvent(event));
        } else {
            this.users.put(id, User.from(event));
        }
    }

    private void consumeEbikesEvent(String message){
        JsonObject obj = new JsonObject(message);
        var event = EbikeEvent.from(obj.getString("ebikeId"), Optional.ofNullable(obj.getString("newState")).map(EbikeState::valueOf),
                                    new V2d(obj.getDouble("deltaPosX"),obj.getDouble("deltaPosY")), new V2d(obj.getDouble("deltaDirX"),obj.getDouble("deltaDirY")),
                                    obj.getDouble("deltaSpeed"), obj.getInteger("deltaBatteryLevel"));
        String id = event.ebikeId();
        if(this.ebikes.containsKey(id)){
            var ebike = this.ebikes.get(id);
            this.ebikes.put(id, ebike.applyEvent(event));
        } else {
            this.ebikes.put(id, Ebike.from(event));
        }
    }

}
