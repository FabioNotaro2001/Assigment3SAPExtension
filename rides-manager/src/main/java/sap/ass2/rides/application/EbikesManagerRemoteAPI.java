package sap.ass2.rides.application;

import java.util.Optional;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.rides.domain.EbikeState;

// Interface that describes the operations that can be done with the ebikes service.
public interface EbikesManagerRemoteAPI {
    Future<Optional<JsonObject>> getBikeByID(String bikeID);
    Future<Void> updateBike(String bikeID, Optional<EbikeState> state, Optional<Double> locationX, Optional<Double> locationY, Optional<Double> directionX, Optional<Double> directionY, Optional<Double> speed, Optional<Integer> batteryLevel);
    Future<JsonArray> getAllEbikes();
}