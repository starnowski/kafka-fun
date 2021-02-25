package com.github.starnowski.kafka.fun;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {KafkaReceiverTest.TOPIC})
public class KafkaReceiverTest {

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
        options = options.assignment(Arrays.asList(new TopicPartition("embedded-test-topic", 0)));
        return KafkaReceiver.create(options);
    }

    @Test
    @DirtiesContext
    public void testReactiveConsumer() {
        // GIVEN
        KafkaReceiver receiver = prepareKafkaReceiver("Test1");
        Random random = new Random();
        String expectedValue = "SSS" + random.nextInt();
        Flux stream = receiver.receive();

        //WHEN
        StepVerifier.FirstStep steps = StepVerifier.create(stream);

        //THEN
        steps.then(() -> {
            try {
                logger.log(Level.INFO, "testReactiveConsumer#kafkaTemplate.send : ");
                kafkaTemplate.send("embedded-test-topic", "KEY", expectedValue).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        })
                .assertNext(r ->
                {
                    logger.log(Level.INFO, "testReactiveConsumer#assertNext: " + r);
                    ConsumerRecord record = (ConsumerRecord) r;
                    Assertions.assertEquals("KEY", record.key());
                    Assertions.assertEquals(expectedValue, record.value());
                })
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));
    }

    @Test
    @DirtiesContext
    public void testReactiveConsumer1() {
        // GIVEN
        KafkaReceiver receiver = prepareKafkaReceiver("Test2");
        Random random = new Random();
        String expectedValue = "YYY" + random.nextInt();

        StepVerifier.create(receiver.receive())
                .then(() -> {
                    try {
                        logger.log(Level.INFO, "testReactiveConsumer1#kafkaTemplate.send : ");
                        kafkaTemplate.send("embedded-test-topic", "KEY", expectedValue).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                })
                .assertNext(r ->
                {
                    logger.log(Level.INFO, "testReactiveConsumer1#assertNext : " + r);
                    ConsumerRecord record = (ConsumerRecord) r;
                    Assertions.assertEquals("KEY", record.key());
                    Assertions.assertEquals(expectedValue, record.value());

                })
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));
    }
}