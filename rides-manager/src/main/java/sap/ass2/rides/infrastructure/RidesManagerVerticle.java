package sap.ass2.rides.infrastructure;

import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import sap.ass2.rides.application.CustomKafkaListener;
import sap.ass2.rides.application.RidesManagerAPI;
import sap.ass2.rides.domain.RideEventConsumer;

public class RidesManagerVerticle extends AbstractVerticle implements RideEventConsumer{
    private int port;
    private RidesManagerAPI ridesAPI;
    
    private static final String RIDES_MANAGER_EVENTS = "rides-manager-events";  // Topic in which the verticle publishes events.
    
    // Possible keys for the getRideByID method.
    private static final String RIDE_ID_TYPE = "ride";
    private static final String USER_ID_TYPE = "user";
    private static final String EBIKE_ID_TYPE = "ebike";

    static Logger logger = Logger.getLogger("[Rides Manager Verticle]");	
    
    public RidesManagerVerticle(int port, RidesManagerAPI ridesAPI, CustomKafkaListener listener) {
        this.port = port;
        this.ridesAPI = ridesAPI;
        listener.onEach("ride-events", this::consumeEvents);
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
            var rides = this.ridesAPI.getAllRides();
            response.put("rides", rides);
            sendReply(context.response(), response);
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
                var ride = this.ridesAPI.beginRide(userID, ebikeID);
                response.put("ride", ride);
                sendReply(context.response(), response);
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
                this.ridesAPI.stopRide(rideID, userID);
                sendReply(context.response(), response);
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
            Function<String, Optional<JsonObject>> getRide;
            
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
            var ride = getRide.apply(id);
            if (ride.isPresent()){
                response.put("ride", ride.get());
            }
            sendReply(context.response(), response);
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
            boolean failed = false;
            JsonObject reply = new JsonObject();

            if (rideID.isEmpty()) { // Request to subscribe on all rides.
                var rides = this.ridesAPI.getAllRides();
                reply.put("rides", rides);
            } else {    // Request to subscribe on a specific ride.
                var ride = this.ridesAPI.getRideByRideID(rideID.get());
                if (ride.isPresent()){
                    reply.put("ride", ride.get());
                } else{
                    webSocket.close();
                    failed = true;
                }
            }

            if(!failed){
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
            }
        });
    }

    @Override
    public void consumeEvents(String message) {
        var eventBus = vertx.eventBus();
        var jsonObj = new JsonObject(message);

        eventBus.publish(RIDES_MANAGER_EVENTS, jsonObj);
    }

}
