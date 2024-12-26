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
 * HTTP client that interacts to the users service.
 */
public class UsersProxy implements UsersAPI {
    private HttpClient client;
	private Vertx vertx;	// Useful for HTTP client implementation.
	private URL appAddress;	// API gateway address.
	
	public UsersProxy(URL appAddress) {
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
    public Future<JsonArray> getAllUsers() {
        Promise<JsonArray> p = Promise.promise();
		client
		.request(HttpMethod.GET, "/api/users")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					JsonObject obj = buf.toJsonObject();
					p.complete(obj.getJsonArray("users"));	// Completes the promise for the admin GUI.
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
    public Future<JsonArray> subscribeToUsersEvents(UserEventObserver observer) {	// observer = admin gui.
        Promise<JsonArray> p = Promise.promise();
		
		// Web socket configuration. We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
		WebSocketConnectOptions wsoptions = new WebSocketConnectOptions()
				  .setHost(this.appAddress.getHost())
				  .setPort(this.appAddress.getPort())
				  .setURI("/api/users/events")
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
						// The message contains the users array.
                        JsonArray users = obj.getJsonArray("users");
                        p.complete(users);
                    } else if (evType.equals("user-update")) {
                        // User parameters to change.
						String userID = obj.getString("userId");
                        int creditChange = obj.getInteger("credits");
						double deltaX = obj.getDouble("deltaX");
						double deltaY = obj.getDouble("deltaY");
                        
						// Notify event to the admin GUI.
						observer.userUpdated(userID, creditChange, deltaX, deltaY);
                    }
                });
            } else {
                p.fail(res.cause());
            }
		});
		
		return p.future();
    }

}
