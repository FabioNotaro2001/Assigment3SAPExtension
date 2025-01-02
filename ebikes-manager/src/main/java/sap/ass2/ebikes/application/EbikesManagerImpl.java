package sap.ass2.ebikes.application;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.ebikes.domain.Ebike.EbikeState;
import sap.ass2.ebikes.domain.EbikeEvent;
import sap.ass2.ebikes.domain.Ebike;
import sap.ass2.ebikes.domain.EbikesRepository;
import sap.ass2.ebikes.domain.P2d;
import sap.ass2.ebikes.domain.RepositoryException;
import sap.ass2.ebikes.domain.V2d;



public class EbikesManagerImpl implements EbikesManagerAPI, EbikeEventsConsumer {
    private static final String EBIKE_EVENTS_TOPIC = "ebike-events";

    // private final EbikesRepository ebikeRepository;
    private final Map<String, Ebike> ebikes;
    private KafkaProducer<String, String> kafkaTemplate;

    private Logger logger = Logger.getLogger("[Ebikes Manager]");

    public EbikesManagerImpl(EbikesRepository ebikeRepository, KafkaProducer<String, String> kafkaTemplate, CustomKafkaListener listener) throws RepositoryException {
        this.ebikes = ebikeRepository.getEbikes().stream().collect(Collectors.toConcurrentMap(b -> b.getId(), Function.identity()));
        this.kafkaTemplate = kafkaTemplate;
        listener.onEach(this::consumeEvents);
    }

    // Converts an ebike to a JSON.
    private static JsonObject toJSON(Ebike ebike) {
        return new JsonObject()
            .put("ebikeId", ebike.getId())
            .put("state", ebike.getState().toString())
            .put("x", ebike.getLocation().x())
            .put("y", ebike.getLocation().y())
            .put("dirX", ebike.getDirection().x())
            .put("dirY", ebike.getDirection().y())
            .put("speed", ebike.getSpeed())
            .put("batteryLevel", ebike.getBatteryLevel());
    }

    private static JsonObject ebikeEventToJSON(String ebikeId, Optional<Ebike.EbikeState> newState, V2d deltaPos, V2d deltaDir, double deltaSpeed, double deltaBatteryLevel) {
        return new JsonObject()
            .put("ebikeId", ebikeId)
            .put("newState", newState.orElse(null))
            .put("deltaPosX", deltaPos.x())
            .put("deltaPosY", deltaPos.y())
            .put("deltaDirX", deltaDir.x())
            .put("deltaDirY", deltaDir.y())
            .put("deltaSpeed", deltaSpeed)
            .put("deltaBatteryLevel", deltaBatteryLevel);
    }

    @Override
    public JsonArray getAllEbikes() {
        return ebikes.values().stream().map(EbikesManagerImpl::toJSON).collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
    }

    @Override
    public JsonArray getAllAvailableEbikesIDs() {
        return ebikes.values().stream().filter(Ebike::isAvailable).map(Ebike::getId).collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
    }

    @Override
    public JsonObject createEbike(String ebikeID, double locationX, double locationY) throws RepositoryException, IllegalArgumentException {
        if (this.ebikes.values().stream().anyMatch(ebike -> ebike.getId().equals(ebikeID))) {
            throw new IllegalArgumentException("Ebike with given id already exists.");
        }

        var ebike = new Ebike(ebikeID, new P2d(locationX, locationY));
        this.kafkaTemplate.send(new ProducerRecord<String,String>(EBIKE_EVENTS_TOPIC, ebikeEventToJSON(ebike.getId(), Optional.of(ebike.getState()), 
        ebike.getLocation().toV2d(), ebike.getDirection(), ebike.getSpeed(), ebike.getBatteryLevel()).encode()));
        return toJSON(ebike);
    }

    @Override
    public void removeEbike(String ebikeID) throws RepositoryException, IllegalArgumentException {
        var ebikeOpt = this.ebikes.values().stream().filter(ebike -> ebike.getId().equals(ebikeID)).findFirst();
        if (ebikeOpt.isEmpty()) { 
            throw new IllegalArgumentException("No ebike with id " + ebikeID);
        }

        var ebike = ebikeOpt.get();
        if (ebike.isInUse()) {
            throw new IllegalStateException("Unable to remove ebike " + ebikeID + ": currently in use");
        }
        this.kafkaTemplate.send(new ProducerRecord<String,String>(EBIKE_EVENTS_TOPIC, ebikeEventToJSON(ebike.getId(), Optional.of(ebike.getState()), V2d.zero(), V2d.zero(), 0, 0).encode()));
    }

    @Override
    public Optional<JsonObject> getEbikeByID(String ebikeID) {
        return this.ebikes.values().stream().filter(ebike -> ebike.getId().equals(ebikeID)).findFirst().map(EbikesManagerImpl::toJSON);
    }

    @Override
    public void updateEbike(String ebikeID, Optional<EbikeState> state, Optional<Double> locationX,
                            Optional<Double> locationY, Optional<Double> directionX, Optional<Double> directionY,
                            Optional<Double> speed, Optional<Integer> batteryLevel) throws RepositoryException, IllegalArgumentException {
        var ebikeOpt = this.ebikes.values().stream().filter(ebike -> ebike.getId().equals(ebikeID)).findFirst();
        if (ebikeOpt.isEmpty()) { 
            throw new IllegalArgumentException("No ebike with id " + ebikeID);
        }

        var ebike = ebikeOpt.get();

        var newState = Optional.<Ebike.EbikeState>empty();
        if (state.isPresent()) {
            newState = Optional.of(state.get());
        }

        var currentState = state.orElse(ebike.getState());

        var deltaPos = V2d.zero();
        if (locationX.isPresent()) {
            deltaPos = deltaPos.sum(new V2d(locationX.get() - ebike.getLocation().x(), 0));
        }
        if (locationY.isPresent()) {
            deltaPos = deltaPos.sum(new V2d(0, locationY.get() - ebike.getLocation().y()));
        }

        var deltaDir = V2d.zero();
        if (directionX.isPresent()) {
            deltaDir = deltaDir.sum(new V2d(directionX.get() - ebike.getDirection().x(), 0));
        }
        if (directionY.isPresent()) {
            deltaDir = deltaDir.sum(new V2d(0, directionY.get() - ebike.getDirection().y()));
        }

        double deltaSpeed = 0;
        if (speed.isPresent()) {
            deltaSpeed += speed.get() - ebike.getSpeed();
        }

        double deltaBatteryLevel = 0;
        if (batteryLevel.isPresent()) {
            deltaBatteryLevel += batteryLevel.get() - ebike.getBatteryLevel();
        }
        this.logger.log(Level.INFO, "Before sending update event");
        this.kafkaTemplate.send(new ProducerRecord<String,String>(EBIKE_EVENTS_TOPIC, ebikeEventToJSON(ebike.getId(), newState, deltaPos, deltaDir, deltaSpeed, deltaBatteryLevel).encode()));
        this.logger.log(Level.INFO, "After sending update event");
    }

    @Override
    public void consumeEvents(String message) {
        JsonObject obj = new JsonObject(message);
        var event = EbikeEvent.from(obj.getString("ebikeId"), Optional.ofNullable(obj.getString("newState")).map(EbikeState::valueOf),
        new V2d(obj.getDouble("deltaPosX"),obj.getDouble("deltaPosY")), new V2d(obj.getDouble("deltaDirX"),obj.getDouble("deltaDirY")),
        obj.getDouble("deltaSpeed"), obj.getInteger("deltaBatteryLevel"));
        if(this.ebikes.containsKey(event.ebikeId())){
            var ebike = this.ebikes.get(event.ebikeId());
            ebike.applyEvent(event);
            if(ebike.getState() == EbikeState.DISMISSED){
                this.ebikes.remove(event.ebikeId());
            }
        } else {
            var ebike = new Ebike(event.ebikeId());
            ebike.applyEvent(event);
            this.ebikes.put(event.ebikeId(), ebike);
        }
    }
}