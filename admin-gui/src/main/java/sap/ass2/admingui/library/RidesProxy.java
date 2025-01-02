package sap.ass2.admingui.library;

import java.net.URL;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * HTTP client that interacts to the rides service.
 */
public class RidesProxy implements RidesAPI {
    private HttpClient client;
	private Vertx vertx;    // Useful for HTTP client implementation.
	private URL appAddress; // API gateway address.
	
	public RidesProxy(URL appAddress) {
        if (Vertx.currentContext() != null) {
			vertx = Vertx.currentContext().owner();
		} else {
			vertx = Vertx.vertx();
		}
        
		this.appAddress = appAddress;
		HttpClientOptions options = new HttpClientOptions()
            .setDefaultHost(appAddress.getHost())
            .setDefaultPort(appAddress.getPort());
		client = vertx.createHttpClient(options);
	}

    @Override
    public Future<JsonArray> getAllRides() {
        Promise<JsonArray> p = Promise.promise();
		client
		.request(HttpMethod.GET, "/api/rides")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
					p.complete(obj.getJsonArray("rides"));  // Completes the promise for the admin GUI.
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
    public Future<JsonArray> subscribeToRideEvents(RideEventObserver observer) {    // observer = admin gui.
        Promise<JsonArray> p = Promise.promise();
		
        // Web socket configuration. We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
		WebSocketConnectOptions wsoptions = new WebSocketConnectOptions()
				  .setHost(this.appAddress.getHost())
				  .setPort(this.appAddress.getPort())
				  .setURI("/api/rides/events")
				  .setAllowOriginHeader(false);
		
		client
		.webSocket(wsoptions)
		.onComplete(res -> {    // Waiting for web socket opening.
            if (res.succeeded()) {
                WebSocket ws = res.result();
                System.out.println("Connected!");
                ws.textMessageHandler(data -> {
                    JsonObject obj = new JsonObject(data);
                    String evType = obj.getString("event");

                    // Event type discovery.
                    if (evType.equals("subscription-started")) {
                        // The message contains the rides array.
                        JsonArray ebikes = obj.getJsonArray("rides");
                        p.complete(ebikes);
                    } else if (evType.equals("ride-start")) {
                        // Rides parameters of the new ride.
                        String rideID = obj.getString("rideId");
                        String userID = obj.getString("userId");
                        String ebikeID = obj.getString("bikeId");
                        
                        // Notify event to the admin GUI.
                        observer.rideStarted(rideID, userID, ebikeID);
                    } else if (evType.equals("ride-step")) {
                        // Ride parameters to change due to step done.
                        String rideID = obj.getString("rideId");
                        Double x = obj.getDouble("x");
                        Double y = obj.getDouble("y");
                        Double directionX = obj.getDouble("dirX");
                        Double directionY = obj.getDouble("dirY");
                        Double speed = obj.getDouble("speed");
                        Integer batteryLevel = obj.getInteger("batteryLevel");
                        
                        // Notify event to the admin GUI.
                        observer.rideStep(rideID, x, y, directionX, directionY, speed, batteryLevel);
                    } else if (evType.equals("ride-end")) {
                        String rideID = obj.getString("rideId");
                        String reason = obj.getString("reason");
                        
                        // Notify event to the admin GUI.
                        observer.rideEnded(rideID, reason);
                    }
                });
            } else {
                p.fail(res.cause());
            }
		});
		
		return p.future();
    }

}
