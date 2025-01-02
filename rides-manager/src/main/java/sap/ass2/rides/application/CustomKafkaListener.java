package sap.ass2.rides.application;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomKafkaListener implements Runnable {
    private final List<String> topics;
    private final KafkaConsumer<String, String> consumer;
    private Map<String, Consumer<String>> recordConsumers;
    static Logger logger = Logger.getLogger("[Kafka Listener]");

    public CustomKafkaListener(KafkaConsumer<String, String> consumer, String... topics) {
        this.topics = List.of(topics);
        this.consumer = consumer;
        this.recordConsumers = this.topics.stream().collect(Collectors.toMap(
                Function.identity(),
                i -> (record -> logger.info("received: " + record))));
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(this.topics);
            while (true) {
                consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> {
                        try {
                            this.recordConsumers.get(record.topic()).accept(record.value());
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Exception", e);
                        }
                    });
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public CustomKafkaListener onEach(String topic, Consumer<String> newConsumer) {
        var temp = this.recordConsumers.get(topic);
        this.recordConsumers.put(topic, temp.andThen(newConsumer));
        return this;
    }
}