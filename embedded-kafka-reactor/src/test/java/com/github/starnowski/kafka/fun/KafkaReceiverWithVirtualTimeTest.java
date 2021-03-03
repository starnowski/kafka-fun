package com.github.starnowski.kafka.fun;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {KafkaReceiverWithVirtualTimeTest.TOPIC})
public class KafkaReceiverWithVirtualTimeTest {

    public static final String TOPIC = "embedded-test-topic";

    private static final Logger logger = Logger.getLogger(KafkaReceiverTest.class.getName());

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;


    private KafkaReceiver prepareKafkaReceiver(String clientId) {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("client.id", clientId);
        optionsMap.put("auto.offset.reset", "earliest");
        optionsMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("bootstrap.servers", embeddedKafkaBroker.getBrokersAsString());
        optionsMap.put("enable.auto.commit", "false");
        optionsMap.put("auto.commit.interval.ms", "1000");
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(optionsMap);
        options = options.assignment(Arrays.asList(new TopicPartition(TOPIC, 0)));
        return KafkaReceiver.create(options);
    }

    @Test
    @Disabled("Hangs")
    @DirtiesContext
    public void testReactiveConsumer() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = new ConstantNumberSupplierWithFailerHandler(2);
        KafkaReceiver<String, String> receiver = prepareKafkaReceiver("Test1");
        Random random = new Random();
        String expectedValue = "SSS" + random.nextInt();

        //WHEN
        Flux stream = receiver.receive().flatMap(rr -> supplierWithFailerHandler.getFlux(rr))
                .retryWhen(Retry.backoff(1, Duration.ofSeconds(2)));

        //THEN
        StepVerifier.withVirtualTime(() -> stream)
//                .expectSubscription()
                .then(() -> {
            try {
                logger.log(Level.INFO, "testReactiveConsumer#kafkaTemplate.send : ");
                kafkaTemplate.send("embedded-test-topic", "KEY", expectedValue).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        })
                .thenAwait(Duration.ofSeconds(2))
                .expectNext(13)
                .assertNext(r ->
                {
                    logger.log(Level.INFO, "testReactiveConsumer#assertNext: " + r);
                    ConsumerRecord record = (ConsumerRecord) r;
                    Assertions.assertEquals("KEY", record.key());
                    Assertions.assertEquals(expectedValue, record.value());
                })
                .verifyComplete();
    }

}