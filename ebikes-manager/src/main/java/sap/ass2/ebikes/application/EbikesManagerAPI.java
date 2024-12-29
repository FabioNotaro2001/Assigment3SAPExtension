package sap.ass2.ebikes.application;

import java.util.Optional;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.ebikes.domain.RepositoryException;
import sap.ass2.ebikes.domain.Ebike.EbikeState;

/**
 * Java interface implemented by EbikesManagerImpl.
 */
public interface EbikesManagerAPI {
    JsonArray getAllEbikes();

    JsonArray getAllAvailableEbikesIDs();

    JsonObject createEbike(String ebikeID, double locationX, double locationY) throws RepositoryException, IllegalArgumentException;

    void removeEbike(String ebikeID) throws RepositoryException, IllegalArgumentException, IllegalStateException;

    Optional<JsonObject> getEbikeByID(String ebikeID);

    void updateEbike(String ebikeID, 
                    Optional<EbikeState> state, 
                    Optional<Double> locationX, Optional<Double> locationY, 
                    Optional<Double> directionX, Optional<Double> directionY, 
                    Optional<Double> speed, 
                    Optional<Integer> batteryLevel) throws RepositoryException, IllegalArgumentException;
}