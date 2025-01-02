package sap.ass2.users;

import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import sap.ass2.users.application.UsersRepositoryImpl;
import sap.ass2.users.application.KafkaProducerFactory;
import sap.ass2.users.application.UsersManagerAPI;
import sap.ass2.users.application.UsersManagerImpl;
import sap.ass2.users.domain.RepositoryException;
import sap.ass2.users.infrastructure.UsersManagerController;
import sap.ass2.users.application.CustomKafkaListener;
import sap.ass2.users.application.KafkaConsumerFactory;

public class UsersManagerService {
    private UsersManagerAPI usersManager;
    private UsersManagerController usersController;
    private URL localAddress;
    private CustomKafkaListener listener;
    private static Logger logger = Logger.getLogger("[Users Service]");

    public UsersManagerService(URL localAddress) throws RepositoryException{
        this.localAddress = localAddress;
        this.listener = new CustomKafkaListener("user-events", KafkaConsumerFactory.defaultConsumer());
        this.usersManager = new UsersManagerImpl(new UsersRepositoryImpl(this.listener), KafkaProducerFactory.kafkaProducer(), this.listener);
        this.listener.onEach(e -> logger.log(Level.INFO, "Received event: " + e));
    }

    public void launch(){
        // Starts the users controller, so that it can starts the service verticle.
        this.usersController = new UsersManagerController(this.localAddress.getPort());
        this.usersController.init(this.usersManager, this.listener);
        CompletableFuture.runAsync(this.listener);
    }
}
