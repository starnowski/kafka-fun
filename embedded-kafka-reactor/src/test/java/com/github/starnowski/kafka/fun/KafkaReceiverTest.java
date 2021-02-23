package com.github.starnowski.kafka.fun;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"embedded-test-topic"}, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class KafkaReceiverTest {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testReactiveConsumer() {
        // GIVEN
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("auto.offset.reset", "earliest");
        optionsMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("bootstrap.servers", "localhost:9092");
        optionsMap.put("enable.auto.commit", "true");
        optionsMap.put("auto.commit.interval.ms", "1000");
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(optionsMap);
        options = options.assignment(Arrays.asList(new TopicPartition("embedded-test-topic", 0)));
        KafkaReceiver receiver = KafkaReceiver.create(options);
        Random random = new Random();
        String expectedValue = "SSS" + random.nextInt();


        //WHEN
        StepVerifier.FirstStep steps = StepVerifier.create(receiver.receive());

        //THEN
        steps.then(() -> {
            try {
                SendResult<String, String> result = kafkaTemplate.send("embedded-test-topic", "KEY", expectedValue).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        })
                .assertNext(r ->
                {
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
    public void testReactiveConsumer1() {
        // GIVEN
        Map<String, Object> optionsMap = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        optionsMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("auto.offset.reset", "earliest");
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(optionsMap);
        options = options.assignment(Arrays.asList(new TopicPartition("embedded-test-topic", 0)));
        KafkaReceiver receiver = KafkaReceiver.create(options);
        Random random = new Random();
        String expectedValue = "YYY" + random.nextInt();

        StepVerifier.create(receiver.receive())
                .then(() -> {
                    try {
                        kafkaTemplate.send("embedded-test-topic", "KEY", expectedValue).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                })
                .assertNext(r ->
                {
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