package sap.ass2.users.application;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.users.domain.RepositoryException;
import sap.ass2.users.domain.User;
import sap.ass2.users.domain.UserEvent;
import sap.ass2.users.domain.UsersRepository;

public class UsersManagerImpl implements UsersManagerAPI, UserEventsConsumer {
    private static final String USER_EVENTS_TOPIC = "user-events";

    // private final UsersRepository userRepository;
    private final Map<String, User> users;
    private KafkaProducer<String, String> kafkaProducer;

    public UsersManagerImpl(UsersRepository userRepository, KafkaProducer<String, String> kafkaProducer, CustomKafkaListener listener) throws RepositoryException {
        this.users = userRepository.getUsers().stream().collect(Collectors.toConcurrentMap(u -> u.getId(), Function.identity()));
        this.kafkaProducer = kafkaProducer;
        listener.onEach(this::consumeEvents);
    }

    // Converts an user to a JSON.
    private static JsonObject toJSON(User user) {
        return new JsonObject()
            .put("userId", user.getId())
            .put("credit", user.getCredit())
            .put("x", user.getX())
            .put("y", user.getY());
    }
    
    private static JsonObject userEventToJSON(String userId, int credits, double x, double y) {
        return new JsonObject()
            .put("userId", userId)
            .put("credits", credits)
            .put("deltaX", x)
            .put("deltaY", y);
    }

    @Override
    public JsonArray getAllUsers() {
        return users.values().stream().map(UsersManagerImpl::toJSON).collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
    }

    @Override
    public JsonObject createUser(String userID) throws RepositoryException {
        var user = new User(userID, 0);
        // this.userRepository.saveUserEvent(user);
        try{
            kafkaProducer.send(new ProducerRecord<String,String>(USER_EVENTS_TOPIC, userEventToJSON(user.getId(), user.getCredit(), 0, 0).encode()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }        
        var result = UsersManagerImpl.toJSON(user);
        return result;
    }

    @Override
    public Optional<JsonObject> getUserByID(String userID) {
        var user = this.users.values().stream().filter(u -> u.getId().equals(userID)).findFirst();
        return user.map(UsersManagerImpl::toJSON);
    }

    @Override
    public void rechargeCredit(String userID, int credit) throws RepositoryException, IllegalArgumentException {
        var userOpt = this.users.values().stream().filter(u -> u.getId().equals(userID)).findFirst();
        if (userOpt.isEmpty()) {
            throw new IllegalArgumentException("Invalid user id");
        }
        kafkaProducer.send(new ProducerRecord<String,String>(USER_EVENTS_TOPIC, userEventToJSON(userID, credit, 0, 0).encode()));
    }

    @Override
    public void decreaseCredit(String userID, int amount) throws RepositoryException {
        var userOpt = this.users.values().stream().filter(u -> u.getId().equals(userID)).findFirst(); 
        if (userOpt.isEmpty()) {
            throw new IllegalArgumentException("Invalid user id");
        }
        kafkaProducer.send(new ProducerRecord<String,String>(USER_EVENTS_TOPIC, userEventToJSON(userID, -amount, 0, 0).encode()));
    }
    
    @Override
    public void move(String userID, double newX, double newY) {
        var userOpt = this.users.values().stream().filter(u -> u.getId().equals(userID)).findFirst(); 
        if (userOpt.isEmpty()) {
            throw new IllegalArgumentException("Invalid user id");
        }
        var user = userOpt.get(); 
        var oldX = user.getX();
        var oldY = user.getY();
        kafkaProducer.send(new ProducerRecord<String,String>(USER_EVENTS_TOPIC, userEventToJSON(userID, 0, newX - oldX, newY - oldY).encode()));
    }

    public void consumeEvents(String message) {
        JsonObject obj = new JsonObject(message);
        var event = UserEvent.from(obj.getString("userId"), obj.getInteger("credits"), obj.getDouble("deltaX"), obj.getDouble("deltaY"));
        if(this.users.containsKey(event.userId())){
            this.users.get(event.userId()).applyEvent(event);
        } else {
            this.users.put(event.userId(), new User(event.userId()).applyEvent(event));
        }
    }
}
