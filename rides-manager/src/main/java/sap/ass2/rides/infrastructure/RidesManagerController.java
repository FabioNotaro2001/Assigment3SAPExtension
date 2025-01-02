package sap.ass2.rides.infrastructure;

import io.vertx.core.Vertx;
import sap.ass2.rides.application.CustomKafkaListener;
import sap.ass2.rides.application.RidesManagerAPI;

/** Class responsible for RidesManagerVerticle deployment. */
public class RidesManagerController {
    private int port;
    private RidesManagerVerticle service;

    public RidesManagerController(int port){
        this.port = port;
    }
    
    public void init(RidesManagerAPI ridesAPI, CustomKafkaListener listener){
        Vertx vertx;
        if (Vertx.currentContext() != null) {
			vertx = Vertx.currentContext().owner();
		} else {
			vertx = Vertx.vertx();
		}
        this.service = new RidesManagerVerticle(this.port, ridesAPI, listener);
        vertx.deployVerticle(this.service);
    }
}
