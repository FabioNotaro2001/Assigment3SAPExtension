package sap.ass2.rides.application;

import java.net.URL;
import java.util.Optional;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import sap.ass2.rides.domain.EbikeState;

/**
 * HTTP client that interacts to the ebikes service.
 */
public class EbikesManagerProxy implements EbikesManagerRemoteAPI {
    private HttpClient client;  
    private Vertx vertx;    // Useful for HTTP client implementation.
	
	public EbikesManagerProxy(URL ebikesManagerAddress) {
        if (Vertx.currentContext() != null) {
			vertx = Vertx.currentContext().owner();
		} else {
			vertx = Vertx.vertx();
		}
        
		HttpClientOptions options = new HttpClientOptions()
            .setDefaultHost(ebikesManagerAddress.getHost())
            .setDefaultPort(ebikesManagerAddress.getPort());
		client = vertx.createHttpClient(options);
	}

    @Override
    public Future<Optional<JsonObject>> getBikeByID(String bikeID) {
        Promise<Optional<JsonObject>> p = Promise.promise();
		client
		.request(HttpMethod.GET, "/api/ebikes/" + bikeID)
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
                    p.complete(Optional.ofNullable(obj.getJsonObject("ebike")));    // Completes the promise for the admin GUI.
				});
			});
			req.send();
		})
		.onFailure(f -> {
			p.fail(f.getMessage());
		});
		return p.future();
    }

    @Override
    public Future<Void> updateBike(String bikeID, Optional<EbikeState> state, Optional<Double> locationX,
            Optional<Double> locationY, Optional<Double> directionX, Optional<Double> directionY,
            Optional<Double> speed, Optional<Integer> batteryLevel) {
            Promise<Void> p = Promise.promise();
            client
            .request(HttpMethod.POST, "/api/ebikes/" + bikeID)
            .onSuccess(req -> {
                req.response().onSuccess(response -> {
                    response.body().onSuccess(buf -> {
                        p.complete();   // Completes the promise for the RidesManager (when starting a new ride) and RidesExecutionVerticle (in all other situations).
                    });
                });

                // Request setup before sending.
                req.putHeader("content-type", "application/json");
                JsonObject body = new JsonObject();
                body.put("ebikeId", bikeID);
                state.ifPresent(s -> body.put("state", s.toString()));
                locationX.ifPresent(locX -> body.put("x", locX));
                locationY.ifPresent(locY -> body.put("y", locY));
                directionX.ifPresent(dirX -> body.put("dirX", dirX));
                directionY.ifPresent(dirY -> body.put("dirY", dirY));
                speed.ifPresent(s -> body.put("speed", s));
                batteryLevel.ifPresent(bl -> body.put("batteryLevel", bl));
                String payload = body.encodePrettily();
                req.putHeader("content-length", "" + payload.length());
                req.write(payload);
                req.send();

            })
            .onFailure(f -> {
                p.fail(f.getMessage());
            });
            return p.future();
    }

    @Override
    public Future<JsonArray> getAllEbikes() {
        Promise<JsonArray> p = Promise.promise();
		client
		.request(HttpMethod.GET, "/api/ebikes")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
					p.complete(obj.getJsonArray("ebikes"));	// Completes the promise for the admin GUI.
				});
			});
			req.send();
		})
		.onFailure(f -> {
			p.fail(f.getMessage());
		});
		return p.future();
    }
}