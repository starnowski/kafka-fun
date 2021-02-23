package com.github.starnowski.kafka.fun;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

@SpringBootTest
@DirtiesContext
// https://blog.mimacom.com/testing-apache-kafka-with-spring-boot-junit5/
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EmbeddedKafka(partitions = 1, topics = {KafkaReceiverTest.TOPIC}, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class KafkaReceiverTest {

    public static final String TOPIC = "embedded-test-topic";

    private static final Logger logger = Logger.getLogger(KafkaReceiverTest.class.getName());

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    KafkaMessageListenerContainer<String, String> container;

    BlockingQueue<ConsumerRecord<String, String>> records;

    @BeforeAll
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("baeldung", "false", embeddedKafkaBroker));
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer());
        ContainerProperties containerProperties = new ContainerProperties(TOPIC);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, String>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @Test
    public void testReactiveConsumer() {
        // GIVEN
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("auto.offset.reset", "latest");
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
                kafkaTemplate.flush();
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
    public void testReactiveConsumer1() {
        // GIVEN
        Map<String, Object> optionsMap = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        optionsMap.put("group.id", "baeldung");
        optionsMap.put("auto.offset.reset", "latest");
        optionsMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        optionsMap.put("bootstrap.servers", "localhost:9092");
        optionsMap.put("enable.auto.commit", "true");
        optionsMap.put("auto.commit.interval.ms", "1000");
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(optionsMap);
        options = options.assignment(Arrays.asList(new TopicPartition("embedded-test-topic", 0)));
        KafkaReceiver receiver = KafkaReceiver.create(options);
        Random random = new Random();
        String expectedValue = "YYY" + random.nextInt();

        StepVerifier.create(receiver.receive())
                .then(() -> {
                    try {
                        kafkaTemplate.send("embedded-test-topic", "KEY", expectedValue).get();
                        kafkaTemplate.flush();
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

    @AfterAll
    void tearDown() {
        container.stop();
    }
}