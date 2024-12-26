package sap.ass2.usergui.library;

import java.net.URL;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;

/**
 * HTTP client that interacts to the rides service.
 */
public class RidesProxy implements RidesAPI {
    private HttpClient client;
	private Vertx vertx;	// Useful for HTTP client implementation.
	private URL appAddress;	// API gateway address.
	private WebSocket webSocket;	// We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
	
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
    public Future<JsonObject> beginRide(String userID, String ebikeID) {
        Promise<JsonObject> p = Promise.promise();
		client
		.request(HttpMethod.POST, "/api/rides")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
					System.out.println("UKAGEFULISGDUFIDSUIFUS : " + obj);
					p.complete(obj.getJsonObject("ride"));	// Completes the promise for the user GUI.
				});
			});

			// Request setup before sending.
            req.putHeader("content-type", "application/json");
			JsonObject body = new JsonObject();
			body.put("userId", userID);
			body.put("ebikeId", ebikeID);
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
    public Future<Void> stopRide(String rideID, String userID) {
        Promise<Void> p = Promise.promise();
		client
		.request(HttpMethod.DELETE, "/api/rides")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					p.complete();	// Completes the promise for the user GUI.
				});
			});

			// Request setup before sending.
            req.putHeader("content-type", "application/json");
			JsonObject body = new JsonObject();
			body.put("rideId", rideID);
			body.put("userId", userID);
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
	public Future<JsonObject> subscribeToRideEvents(String rideId, RideEventObserver observer) {	// observer = user gui.
		Promise<JsonObject> p = Promise.promise();
		
		// Web socket configuration. We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
		WebSocketConnectOptions wsoptions = new WebSocketConnectOptions()
				  .setHost(this.appAddress.getHost())
				  .setPort(this.appAddress.getPort())
				  .setURI("/api/rides/" + rideId + "/events")
				  .setAllowOriginHeader(false);
		
		client
		.webSocket(wsoptions)
		.onComplete(res -> {	// Waiting for web socket opening.
            if (res.succeeded()) {
                this.webSocket = res.result();
                System.out.println("Connected!");
                this.webSocket.textMessageHandler(data -> {
                    JsonObject obj = new JsonObject(data);
                    String evType = obj.getString("event");
					// Event type discovery.
                    if (evType.equals("subscription-started")) {
                        // The message contains the ride to watch.
						JsonObject ebike = obj.getJsonObject("ebike");
                        p.complete(ebike);
                    } else if (evType.equals("ride-step")) {
						// Ride parameters of the ride that has made a step.
						Double x = obj.getDouble("x");
                        Double y = obj.getDouble("y");
						Double directionX = obj.getDouble("dirX");
						Double directionY = obj.getDouble("dirY");
						Double speed = obj.getDouble("speed");
						Integer batteryLevel = obj.getInteger("batteryLevel");
						
						// Notify event to the user GUI.
						observer.rideStep(rideId, x, y, directionX, directionY, speed, batteryLevel);
                    } else if (evType.equals("ride-end")) {
                        String reason = obj.getString("reason");

						// Notify event to the user GUI.
                        observer.rideEnded(rideId, reason);
                    }
                });
            } else {
                p.fail(res.cause());
            }
		});
		
		return p.future();
	}

	@Override
	public void unsubscribeFromRideEvents() {
		// Writes a message to the rides proxy of the rides service asking for unsubscription.
		this.webSocket.writeTextMessage("unsubscribe")
			.onComplete(h -> {
				this.webSocket.close();
				this.webSocket = null;
			});
	}
}
