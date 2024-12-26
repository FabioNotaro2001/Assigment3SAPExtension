package sap.ass2.rides;

import java.net.URL;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import sap.ass2.rides.application.CustomKafkaListener;
import sap.ass2.rides.application.EbikesManagerRemoteAPI;
import sap.ass2.rides.application.Environment;
import sap.ass2.rides.application.KafkaConsumerFactory;
import sap.ass2.rides.application.RidesManagerAPI;
import sap.ass2.rides.application.RidesManagerImpl;
import sap.ass2.rides.application.UsersManagerRemoteAPI;
import sap.ass2.rides.infrastructure.RidesManagerController;

public class RidesManagerService {
    private RidesManagerAPI ridesManager;
    private RidesManagerController ridesController;
    private URL localAddress;

    public RidesManagerService(URL localAddress, UsersManagerRemoteAPI usersManager, EbikesManagerRemoteAPI ebikesManager) {
        this.localAddress = localAddress;
        Environment environment = new Environment(new CustomKafkaListener("user-events", KafkaConsumerFactory.defaultConsumer()), new CustomKafkaListener("ebike-events", KafkaConsumerFactory.defaultConsumer()));

        this.ridesManager = new RidesManagerImpl(usersManager, ebikesManager, environment);

        var futureForUsers = usersManager.getAllUsers();
        var futureForEbikes = ebikesManager.getAllEbikes();

        Future.all(futureForUsers, futureForEbikes).onSuccess((cf) -> {
            List<JsonArray> results = cf.list();
            environment.init(results.get(0), results.get(1));
        });

    }

    public void launch(){
        // Starts the ride controller (so that it can start the RidesExecutionVerticle).
        this.ridesController = new RidesManagerController(this.localAddress.getPort());
        this.ridesController.init(this.ridesManager);
    }
}
