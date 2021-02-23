package com.github.starnowski.kafka.fun;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaUtils;
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
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"embedded-test-topic"}, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
public class KafkaReceiverTest {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testReactiveConsumer() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("auto.offset.reset", "earliest");
        optionsMap.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("bootstrap.servers","localhost:9092");
        optionsMap.put("enable.auto.commit", "true");
        optionsMap.put("auto.commit.interval.ms", "1000");
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(optionsMap);
        options = options.assignment(Arrays.asList(new TopicPartition("embedded-test-topic", 1)));
        KafkaReceiver receiver = KafkaReceiver.create(options);


        StepVerifier.create(receiver.receive())
                .then(() -> {
//                    try {
//                        rec = producer.send(
//                                new ProducerRecord<String, String>("embedded-test-topic", "KEY", "VALUE"))
//                                .get();
//                        kafkaTemplate.send()
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    } catch (ExecutionException e) {
//                        e.printStackTrace();
//                    }
                    try {
                        SendResult<String, String> result = kafkaTemplate.send("embedded-test-topic", "KEY", "VALUE").get();
                        kafkaTemplate.flush();
                        System.out.println(result.getProducerRecord());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
//                    return rec;
                })
                .expectNext("VALUE")
//                .assertNext(record -> {
//                    assertEquals("KEY", record.get);
//                    assertEquals("VALUE", record.value());
//                })
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));
    }

    @Test
    public void testReactiveConsumer1() {

        Map<String, Object> optionsMap = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        optionsMap.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("auto.offset.reset", "earliest");
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(optionsMap);
        options = options.assignment(Arrays.asList(new TopicPartition("embedded-test-topic", 0)));
        KafkaReceiver receiver = KafkaReceiver.create(options);

        StepVerifier.create(receiver.receive())
                .then(() -> {
//                    try {
//                        rec = producer.send(
//                                new ProducerRecord<String, String>("embedded-test-topic", "KEY", "VALUE"))
//                                .get();
//                        kafkaTemplate.send()
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    } catch (ExecutionException e) {
//                        e.printStackTrace();
//                    }
                    try {
                        SendResult<String, String> result = kafkaTemplate.send("embedded-test-topic", "KEY", "VALUE").get();
                        kafkaTemplate.flush();
                        System.out.println(result.getProducerRecord());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
//                    return rec;
                })
                .expectNext("VALUE")
//                .assertNext(record -> {
//                    assertEquals("KEY", record.get);
//                    assertEquals("VALUE", record.value());
//                })
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));
    }
}