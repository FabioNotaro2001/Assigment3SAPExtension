package sap.ass2.rides.infrastructure;

import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import sap.ass2.rides.application.RidesManagerAPI;
import sap.ass2.rides.domain.RideEventObserver;

public class RidesManagerVerticle extends AbstractVerticle implements RideEventObserver {
    private int port;
    private RidesManagerAPI ridesAPI;
    
    private static final String RIDES_MANAGER_EVENTS = "rides-manager-events";  // Topic in which the verticle publishes events.
    
    // Possible keys for the getRideByID method.
    private static final String RIDE_ID_TYPE = "ride";
    private static final String USER_ID_TYPE = "user";
    private static final String EBIKE_ID_TYPE = "ebike";
    
    // Events that this verticle can publish.
    private static final String START_EVENT = "ride-start";
    private static final String STEP_EVENT = "ride-step";
    private static final String END_EVENT = "ride-end";

    static Logger logger = Logger.getLogger("[Rides Manager Verticle]");	
    
    public RidesManagerVerticle(int port, RidesManagerAPI ridesAPI) {
        this.port = port;
        this.ridesAPI = ridesAPI;
    }

    public void start() {
        HttpServer server = vertx.createHttpServer();   // Creates HTTP server to handle the requests coming from the external proxies.
        Router router = Router.router(vertx);
        
        router.route(HttpMethod.GET, "/api/rides").handler(this::getAllRides);
        router.route(HttpMethod.POST, "/api/rides").handler(this::beginRide);
        router.route(HttpMethod.DELETE, "/api/rides").handler(this::stopRide);
        router.route("/api/rides/events").handler(this::handleEventSubscription);
        router.route(HttpMethod.GET, "/api/rides/:rideId").handler(this::getRideByID);
        router.route("/api/rides/:rideId/events").handler(this::handleEventSubscription);
        
        server.requestHandler(router).listen(this.port);
    }

    private static void sendReply(HttpServerResponse response, JsonObject reply) {
        response.putHeader("content-type", "application/json");
        response.end(reply.toString());
    }

    private static void sendServiceError(HttpServerResponse response, Exception ex) {
        logger.log(Level.SEVERE, "Exception", ex);
        response.setStatusCode(500);
        response.putHeader("content-type", "application/json");

        JsonObject err = new JsonObject();
        err.put("error", Optional.ofNullable(ex.getMessage()).orElse(ex.toString()));
        response.end(err.toString());    
    }

    private static void sendBadRequest(HttpServerResponse response, Exception ex) {
        response.setStatusCode(400);
        response.putHeader("content-type", "application/json");

        JsonObject err = new JsonObject();
        err.put("error", Optional.ofNullable(ex.getMessage()).orElse(ex.toString()));
        response.end(err.toString());    
    }

    protected void getAllRides(RoutingContext context) {
        logger.log(Level.INFO, "Received 'getAllRides'");

        JsonObject response = new JsonObject();
        try {
            this.ridesAPI.getAllRides().onSuccess(rides -> {
                response.put("rides", rides);
                sendReply(context.response(), response);
            });
        } catch (Exception ex) {
            sendServiceError(context.response(), ex);
        }
    }

    protected void beginRide(RoutingContext context) {
        logger.log(Level.INFO, "Received 'beginRide'");

        context.request().handler(buffer -> {
            JsonObject data = buffer.toJsonObject();
            String userID = data.getString("userId");
            String ebikeID = data.getString("ebikeId");
            JsonObject response = new JsonObject();
            try {
                this.ridesAPI.beginRide(userID, ebikeID).onSuccess(ride -> {
                    response.put("ride", ride);
                    sendReply(context.response(), response);
                });
            } catch (Exception ex) {
                sendServiceError(context.response(), ex);
            }
        });
    }

    protected void stopRide(RoutingContext context) {
        logger.log(Level.INFO, "Received 'stopRide'");

        context.request().handler(buffer -> {
            JsonObject data = buffer.toJsonObject();
            String rideID = data.getString("rideId");
            String userID = data.getString("userId");
            JsonObject response = new JsonObject();
            try {
                this.ridesAPI.stopRide(rideID, userID).onSuccess(v -> {
                    sendReply(context.response(), response);
                });
            } catch (Exception ex) {
                sendServiceError(context.response(), ex);
            }
        });
    }

    protected void getRideByID(RoutingContext context) {
        logger.log(Level.INFO, "Received 'getRideByID'");

        String idType = context.pathParam("IdType");
        String id = context.pathParam("id");
        JsonObject response = new JsonObject();
        try {
            Function<String, Future<Optional<JsonObject>>> getRide;

            switch (idType) {
                case RIDE_ID_TYPE: {
                    getRide = this.ridesAPI::getRideByRideID;
                    break;
                }
                case USER_ID_TYPE: {
                    getRide = this.ridesAPI::getRideByUserID;
                    break;
                }
                case EBIKE_ID_TYPE: {
                    getRide = this.ridesAPI::getRideByEbikeID;
                    break;
                }
                default: {
                    sendBadRequest(context.response(), new IllegalArgumentException("Invalid IdType"));
                    return;
                }
            }
            getRide.apply(id).onSuccess(ride -> {
                if (ride.isPresent()){
                    response.put("ride", ride.get());
                }
                sendReply(context.response(), response);
            });
        } catch (Exception ex) {
            sendServiceError(context.response(), ex);
        }
    }

    // Handle the request of a proxy to subscribe for the events related to a specified ride or all rides.
    protected void handleEventSubscription(RoutingContext context){
        logger.log(Level.INFO, "Received subscription request");

        Optional<String> rideID = Optional.ofNullable(context.pathParam("rideId"));

        HttpServerRequest request = context.request();
        var wsFuture = request.toWebSocket();
        wsFuture.onSuccess(webSocket -> {   // Web socket configuration. We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
            JsonObject reply = new JsonObject();
            Future<Void> fut = null;

            if (rideID.isEmpty()) { // Request to subscribe on all rides.
                fut = this.ridesAPI.getAllRides().onSuccess(rides -> {
                    reply.put("rides", rides);
                }).compose(rides -> Future.succeededFuture());
            } else {    // Request to subscribe on a specific ride.
                fut = this.ridesAPI.getRideByRideID(rideID.get()).map(ride -> {
                    if (ride.isPresent()){
                        reply.put("ride", ride.get());
                        return true;
                    } else{
                        webSocket.close();
                        return false;
                    }
                }).compose(success -> success ? Future.succeededFuture() : Future.failedFuture(new Throwable()));
            }

            fut.onSuccess(s -> {
                reply.put("event", "subscription-started"); // Sends back the response.
                webSocket.writeTextMessage(reply.encodePrettily());

                var eventBus = vertx.eventBus();
                var consumer = eventBus.consumer(RIDES_MANAGER_EVENTS, msg -> { // Specifies the behavior to do when this verticles listens an event on the bus.            
                    JsonObject ride = (JsonObject) msg.body();
                    if(rideID.isEmpty() || rideID.get().equals(ride.getString("rideId"))){
                        logger.log(Level.INFO, "Sending event " + ride.getString("event"));
                        
                        webSocket.writeTextMessage(ride.encodePrettily());
                    }
                });
                
                // Listens on web socket for unsubscribe messages.
                webSocket.textMessageHandler(data -> {
                    if(data.equals("unsubscribe")){
                        consumer.unregister();
                        webSocket.close();
                    }
                });
            });
        });
    }

    @Override
    public void rideStarted(String rideID, String userID, String ebikeID) {
        var eventBus = vertx.eventBus();
        var obj = new JsonObject()
            .put("event", START_EVENT)
            .put("rideId", rideID)
            .put("userId", userID)
            .put("ebikeId", ebikeID);
        eventBus.publish(RIDES_MANAGER_EVENTS, obj);
    }

    @Override
    public void rideStep(String rideID, double x, double y, double directionX, double directionY, double speed, int batteryLevel) {
        var eventBus = vertx.eventBus();
        var obj = new JsonObject()
            .put("event", STEP_EVENT)
            .put("rideId", rideID)
            .put("x", x)
            .put("y", y)
            .put("dirX", directionX)
            .put("dirY", directionY)
            .put("speed", speed)
            .put("batteryLevel", batteryLevel);
        eventBus.publish(RIDES_MANAGER_EVENTS, obj);
    }

    @Override
    public void rideEnded(String rideID, String reason) {
        var eventBus = vertx.eventBus();
        var obj = new JsonObject()
            .put("event", END_EVENT)
            .put("rideId", rideID)
            .put("reason", reason);
        eventBus.publish(RIDES_MANAGER_EVENTS, obj);
    }

}
