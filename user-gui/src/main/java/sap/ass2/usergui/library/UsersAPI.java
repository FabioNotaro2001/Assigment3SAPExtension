package sap.ass2.usergui.library;

import java.util.Optional;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface UsersAPI {
    Future<JsonArray> getAllUsers();
    Future<JsonObject> createUser(String userID);
    Future<Optional<JsonObject>> getUserByID(String userID);
    Future<Void> rechargeCredit(String userID, int credit);
    Future<Void> move(String userID, double newX, double newY);

    /**
     * Allows the user GUI to subscribe to the user events.
     * @param userID
     * @param observer
     * @return
     */
    Future<JsonObject> subscribeToUserEvents(String userID, UserEventObserver observer);
}
