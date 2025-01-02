package sap.ass2.ebikes.infrastructure;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import sap.ass2.ebikes.application.EbikeEventsConsumer;
import sap.ass2.ebikes.application.EbikesManagerAPI;
import sap.ass2.ebikes.domain.Ebike.EbikeState;
import sap.ass2.ebikes.application.CustomKafkaListener;

public class EbikesManagerVerticle extends AbstractVerticle implements EbikeEventsConsumer {
    private int port;
    private EbikesManagerAPI ebikesAPI;
    private static final String EBIKES_MANAGER_EVENTS = "ebikes-manager-events";    // Topic in which the verticle publishes events.
    
    // Events that this verticle can publish.
    private static final String UPDATE_EVENT = "ebike-update";
    private static final String REMOVE_EVENT = "ebike-remove";

    static Logger logger = Logger.getLogger("[Ebikes Manager Verticle]");	

    public EbikesManagerVerticle(int port, EbikesManagerAPI ebikesAPI, CustomKafkaListener listener) {
        this.port = port;
        this.ebikesAPI = ebikesAPI;
        listener.onEach(this::consumeEvents);
    }

    public void start() {
        HttpServer server = vertx.createHttpServer();   // Creates HTTP server to handle the requests coming from the external proxies.
        Router router = Router.router(vertx);
        
        router.route(HttpMethod.GET, "/api/ebikes").handler(this::getAllEbikes);
        router.route(HttpMethod.GET, "/api/ebikes/ids").handler(this::getAllAvailableEbikesIds);
        router.route(HttpMethod.POST, "/api/ebikes").handler(this::createEbike);
        router.route("/api/ebikes/events").handler(this::handleEventSubscription);
        router.route(HttpMethod.GET, "/api/ebikes/:ebikeId").handler(this::getEbikeByID);
        router.route(HttpMethod.DELETE, "/api/ebikes/:ebikeId").handler(this::deleteEbike);
        router.route(HttpMethod.POST, "/api/ebikes/:ebikeId").handler(this::updateEbike);
        router.route("/api/ebikes/:ebikeId/events").handler(this::handleEventSubscription);
        
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

    protected void getAllEbikes(RoutingContext context) {
        logger.log(Level.INFO, "Received 'getAllEbikes'");
        
        JsonObject response = new JsonObject();
        try {
            response.put("ebikes", this.ebikesAPI.getAllEbikes());
            sendReply(context.response(), response);
        } catch (Exception ex) {
            sendServiceError(context.response(), ex);
        }
    }
    
    protected void getAllAvailableEbikesIds(RoutingContext context) {
        logger.log(Level.INFO, "Received 'getAllAvailableEbikesIds'");

        JsonObject response = new JsonObject();
        try {
            response.put("ebikes", this.ebikesAPI.getAllAvailableEbikesIDs());
            sendReply(context.response(), response);
        } catch (Exception ex) {
            sendServiceError(context.response(), ex);
        }
    }

    protected void createEbike(RoutingContext context) {
        logger.log(Level.INFO, "Received 'createEbike'");

        context.request().handler(buffer -> {
            JsonObject data = buffer.toJsonObject();
            String ebikeID = data.getString("ebikeId");
            double x = data.getDouble("x");
            double y = data.getDouble("y");
            
            JsonObject response = new JsonObject();
            try {
                response.put("ebike", this.ebikesAPI.createEbike(ebikeID, x, y));
                sendReply(context.response(), response);
            } catch (Exception ex) {
                sendServiceError(context.response(), ex);
            }
        });
    }

    protected void deleteEbike(RoutingContext context) {
        logger.log(Level.INFO, "Received 'deleteEbike'");

        JsonObject response = new JsonObject();
        String ebikeID = context.request().getParam("ebikeId");
        try {
            this.ebikesAPI.removeEbike(ebikeID);
            sendReply(context.response(), response);
        } catch (Exception ex) {
            sendServiceError(context.response(), ex);
        }
    }

    protected void getEbikeByID(RoutingContext context) {
        logger.log(Level.INFO, "Received 'getEbikeByID'");

        String ebikeID = context.pathParam("ebikeId");
        JsonObject response = new JsonObject();
        try {
            var ebikeOpt = this.ebikesAPI.getEbikeByID(ebikeID);
            if (ebikeOpt.isPresent()) {
                response.put("ebike", ebikeOpt.get());
            }
            sendReply(context.response(), response);
        } catch (Exception ex) {
            sendServiceError(context.response(), ex);
        }
    }

    protected void updateEbike(RoutingContext context) {
        logger.log(Level.INFO, "Received 'updateEbike'");

        context.request().handler(buffer -> {
            JsonObject data = null;
            try{
                data = buffer.toJsonObject();
            } catch (Exception e){
                sendServiceError(context.response(), e);
                return;
            }
            String ebikeID = context.pathParam("ebikeId");
            Optional<EbikeState> state;
            Optional<Double> x, y, dirX, dirY, speed;
            Optional<Integer> batteryLevel;
            try {
                state = Optional.ofNullable(data.getString("state")).map(EbikeState::valueOf);
                x = Optional.ofNullable(data.getDouble("x"));
                y = Optional.ofNullable(data.getDouble("y"));
                dirX = Optional.ofNullable(data.getDouble("dirX"));
                dirY = Optional.ofNullable(data.getDouble("dirY"));
                speed = Optional.ofNullable(data.getDouble("speed"));
                batteryLevel = Optional.ofNullable(data.getInteger("batteryLevel"));
            } catch (Exception ex) {
                sendBadRequest(context.response(), ex);
                return;
            }

            JsonObject response = new JsonObject();
            try {
                this.ebikesAPI.updateEbike(ebikeID, state, x, y, dirX, dirY, speed, batteryLevel);
                sendReply(context.response(), response);
            } catch (Exception ex) {
                sendServiceError(context.response(), ex);
            }
        });
    }

    // Handle the request of an external proxy to subscribe for the events related to a specified bike or all bikes.
    protected void handleEventSubscription(RoutingContext context){
        logger.log(Level.INFO, "Received event subscription request");

        Optional<String> ebikeID = Optional.ofNullable(context.pathParam("ebikeId"));
        HttpServerRequest request = context.request();
        var wsFuture = request.toWebSocket();
        wsFuture.onSuccess(webSocket -> {   // Web socket configuration. We use web socket because we want to estabilish a more sophisticated connection, not a simple request/response.
            JsonObject reply = new JsonObject();
            
            if(ebikeID.isEmpty()){  // Request to subscribe on all bikes.
                JsonArray ebikes = this.ebikesAPI.getAllEbikes();
                reply.put("ebikes", ebikes);
            } else{ // Request to subscribe on a specific bike.
                Optional<JsonObject> ebike = this.ebikesAPI.getEbikeByID(ebikeID.get());
                if (ebike.isPresent()){
                    reply.put("ebike", ebike.get());
                } else{
                    webSocket.close();
                    return;
                }
            }

            reply.put("event", "subscription-started");
            webSocket.writeTextMessage(reply.encodePrettily()); // Sends back the response.

            var eventBus = vertx.eventBus();
            var consumer = eventBus.consumer(EBIKES_MANAGER_EVENTS, msg -> {    // Specifies the behavior to do when this verticles listens an event on the bus.            
                JsonObject ebike = (JsonObject) msg.body();
                if(ebikeID.isEmpty() || ebikeID.get().equals(ebike.getString("ebikeId"))){
                    logger.log(Level.INFO, "Sending event");

                    webSocket.writeTextMessage(ebike.encodePrettily());
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
    }

    @Override
    public void consumeEvents(String message) {
        var eventBus = vertx.eventBus();
        var jsonObj = new JsonObject(message);
        var newState = Optional.ofNullable(jsonObj.getString("newState"));
        if (newState.isPresent() && EbikeState.valueOf(newState.get()) == EbikeState.DISMISSED) {
            jsonObj.put("event", REMOVE_EVENT);
        } else {
            jsonObj.put("event", UPDATE_EVENT);
        }
        eventBus.publish(EBIKES_MANAGER_EVENTS, jsonObj);
    }
}