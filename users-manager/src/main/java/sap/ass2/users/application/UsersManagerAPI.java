package sap.ass2.users.application;

import java.util.Optional;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.users.domain.RepositoryException;
import sap.ass2.users.domain.UserEventObserver;

/**
 * Java interface implemented by UsersManagerImpl.
 */
public interface UsersManagerAPI {
    JsonArray getAllUsers();

    JsonObject createUser(String userID) throws RepositoryException;

    Optional<JsonObject> getUserByID(String userID);

    void rechargeCredit(String userID, int credit) throws RepositoryException, IllegalArgumentException;

    void decreaseCredit(String userID, int amount) throws RepositoryException;

    void move(String userID, double newX, double newY);

    /**
     * Allows the observer (UsersManagerVerticle, so indirectly the other services) to watch out the users.
     * @param observer
     */
    void subscribeToUserEvents(UserEventObserver observer);
}
