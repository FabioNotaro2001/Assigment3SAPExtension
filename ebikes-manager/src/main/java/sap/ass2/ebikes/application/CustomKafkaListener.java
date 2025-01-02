package sap.ass2.ebikes.application;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomKafkaListener implements Runnable {
    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private Consumer<String> recordConsumer;
    static Logger logger = Logger.getLogger("[Kafka Listener]");

    public CustomKafkaListener(String topic, KafkaConsumer<String, String> consumer) {
        this.topic = topic;
        this.consumer = consumer;
        this.recordConsumer = record -> logger.info("received: " + record);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> {
                        try {
                            recordConsumer.accept(record.value());
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Exception", e);
                        }
                    });
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public CustomKafkaListener onEach(Consumer<String> newConsumer) {
        recordConsumer = recordConsumer.andThen(newConsumer);
        return this;
    }
}