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

/**
 * HTTP client that interacts to the users service.
 */
public class UsersManagerProxy implements UsersManagerRemoteAPI {
    private HttpClient client;
	private Vertx vertx;	// Useful for HTTP client implementation.
	
	public UsersManagerProxy(URL usersManagerAddress) {
		if (Vertx.currentContext() != null) {
			vertx = Vertx.currentContext().owner();
		} else {
			vertx = Vertx.vertx();
		}

		HttpClientOptions options = new HttpClientOptions()
            .setDefaultHost(usersManagerAddress.getHost())
            .setDefaultPort(usersManagerAddress.getPort());
		client = vertx.createHttpClient(options);
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
					p.complete(Optional.ofNullable(obj.getJsonObject("user")));
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
	public Future<Void> decreaseCredit(String userID, int amount) {
		Promise<Void> p = Promise.promise();
		client
		.request(HttpMethod.POST, "/api/users/" + userID + "/decrease-credit")
		.onSuccess(req -> {
			req.response().onSuccess(response -> {
				response.body().onSuccess(buf -> {
					p.complete();	// Completes the promise for the RidesExecutionVerticle.
				});
			});

			// Request setup before sending.
			req.putHeader("content-type", "application/json");
			JsonObject body = new JsonObject();
			body.put("userId", userID);
			body.put("credit", amount);
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
}