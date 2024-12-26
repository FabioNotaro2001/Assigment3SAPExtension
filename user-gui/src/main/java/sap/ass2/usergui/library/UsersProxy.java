package sap.ass2.usergui.library;

import java.net.URL;
import java.util.Optional;
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
 * HTTP client that interacts to the users service.
 */
public class UsersProxy implements UsersAPI {
    private HttpClient client;
	private Vertx vertx;	// Useful for HTTP client implementation.
	private URL appAddress;	// API gateway address.
	
	public UsersProxy(URL usersManagerAddress) {
		if (Vertx.currentContext() != null) {
			vertx = Vertx.currentContext().owner();
		} else {
			vertx = Vertx.vertx();
		}
		
		this.appAddress = usersManagerAddress;
		HttpClientOptions options = new HttpClientOptions()
			.setDefaultHost(usersManagerAddress.getHost())
			.setDefaultPort(usersManagerAddress.getPort());
		client = vertx.createHttpClient(options);
	}

    @Override
    public Future<JsonArray> getAllUsers() {
        Promise<JsonArray> p = Promise.promise();
		client
		.request(HttpMethod.GET, "/api/users")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					try{
						JsonObject obj = buf.toJsonObject();
						p.complete(obj.getJsonArray("users"));	// Completes the promise for the user GUI.
					} catch (Exception e) {
						System.out.println("Errore: " + buf.toString());
						p.fail(e);
					}
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
	public Future<JsonObject> createUser(String userID) {
		Promise<JsonObject> p = Promise.promise();
		client
		.request(HttpMethod.POST, "/api/users")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
					System.out.println(obj);
					p.complete(obj.getJsonObject("user"));	// Completes the promise for the user GUI.
				});
			});

			// Request setup before sending.
            req.putHeader("content-type", "application/json");
			JsonObject body = new JsonObject();
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
	public Future<Optional<JsonObject>> getUserByID(String userID) {
		Promise<Optional<JsonObject>> p = Promise.promise();
		client
		.request(HttpMethod.GET, "/api/users/" + userID)
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
					p.complete(Optional.ofNullable(obj.getJsonObject("user")));	// Completes the promise for the user GUI.
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
	public Future<Void> rechargeCredit(String userID, int credit) {
		Promise<Void> p = Promise.promise();
            client
            .request(HttpMethod.POST, "/api/users/" + userID + "/recharge-credit")
            .onSuccess(req -> {
                req.response().onSuccess(response -> {
                    response.body().onSuccess(buf -> {
                        p.complete();	// Completes the promise for the user GUI.
                    });
                });

				// Request setup before sending.
                req.putHeader("content-type", "application/json");
                JsonObject body = new JsonObject();
                body.put("credit", credit);
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
	public Future<Void> move(String userID, double newX, double newY) {
		Promise<Void> p = Promise.promise();
            client
            .request(HttpMethod.POST, "/api/users/" + userID + "/move")
            .onSuccess(req -> {
                req.response().onSuccess(response -> {
                    response.body().onSuccess(buf -> {
                        p.complete();	// Completes the promise for the user GUI.
                    });
                });

				// Request setup before sending.
                req.putHeader("content-type", "application/json");
                JsonObject body = new JsonObject();
                body.put("x", newX);
				body.put("y", newY);
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
	public Future<JsonObject> subscribeToUserEvents(String userID, UserEventObserver observer) {	// observer = user gui.
		Promise<JsonObject> p = Promise.promise();
		
		// Web socket configuration. We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
		WebSocketConnectOptions wsoptions = new WebSocketConnectOptions()
				  .setHost(this.appAddress.getHost())
				  .setPort(this.appAddress.getPort())
				  .setURI("/api/users/" + userID + "/events")
				  .setAllowOriginHeader(false);
		
		client
		.webSocket(wsoptions)
		.onComplete(res -> {
            if (res.succeeded()) {
                WebSocket ws = res.result();
                System.out.println("Connected!");
                ws.textMessageHandler(data -> {
                    JsonObject obj = new JsonObject(data);
                    String evType = obj.getString("event");
                    
					// Event type discovery.
					if (evType.equals("subscription-started")) {
                        // The message contains the user to watch.
						JsonObject user = obj.getJsonObject("user");
                        p.complete(user);
                    } else if (evType.equals("user-update")) {
                        int credit = obj.getInteger("credits");
                        
						// Notify event to the admin GUI.
						observer.userUpdated(userID, credit);
                    }
                });
            } else {
                p.fail(res.cause());
            }
		});
		
		return p.future();
	}
}
