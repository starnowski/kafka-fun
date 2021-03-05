package com.github.starnowski.kafka.fun;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 3, topics = {KafkaReceiverWithDifferentTopicsWithGenericHandlerTest.TOPIC_1, KafkaReceiverWithDifferentTopicsWithGenericHandlerTest.TOPIC_2, KafkaReceiverWithDifferentTopicsWithGenericHandlerTest.TOPIC_3})
public class KafkaReceiverWithDifferentTopicsWithGenericHandlerTest {

    public static final String TOPIC_1 = "first-embedded-test-topic";
    public static final String TOPIC_2 = "second-embedded-test-topic";
    public static final String TOPIC_3 = "third-embedded-test-topic";

    private static final Logger logger = Logger.getLogger(KafkaReceiverTest.class.getName());

    private static final int MAX_ATTEMPTS = 7;
    private static final int MAX_DELAY_IN_SECONDS = 2;

    private PipelineFactory tested = new PipelineFactory();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;


    private KafkaReceiver prepareKafkaReceiver(String clientId, String topic, int partition) {
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
        options = options.assignment(Arrays.asList(new TopicPartition(topic, partition)));
        return KafkaReceiver.create(options);
    }

    @Test
    public void testReactiveConsumer() {
        // GIVEN
        GenericFunction<String, String> handler = new GenericFunction<>();
        KafkaReceiver receiver = prepareKafkaReceiver("Test1", TOPIC_1, 0);
        Random random = new Random();
        String expectedValue = "SSS" + random.nextInt();
        String expectedKey = "KEY11" + random.nextInt();
        Flux source = receiver.receive();

        // WHEN
        Flux<String> stream = tested.testedPipeline(source, handler, MAX_ATTEMPTS, MAX_DELAY_IN_SECONDS, RecoverableException.class);

        //THEN
        StepVerifier.create(stream).then(() -> {
            try {
                logger.log(Level.INFO, "testReactiveConsumer#kafkaTemplate.send : ");
                kafkaTemplate.send(TOPIC_1, 0, expectedKey, expectedValue).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        })
                .assertNext(r ->
                {
                    logger.log(Level.INFO, "testReactiveConsumer#assertNext: " + r);
                    Assertions.assertEquals(expectedValue, r);
                })
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));
    }

    @Test
    public void testReactiveConsumer1() {
        // GIVEN
        GenericFunction<String, String> handler = new GenericFunction<>();
        KafkaReceiver source = prepareKafkaReceiver("Test2", TOPIC_2, 1);
        Random random = new Random();
        String expectedValue = "YYY" + random.nextInt();
        String expectedKey = "KEYXXZ" + random.nextInt();

        // WHEN
        Flux<String> stream = tested.testedPipeline(source.receive(), handler, MAX_ATTEMPTS, MAX_DELAY_IN_SECONDS, RecoverableException.class);

        StepVerifier.create(stream)
                .then(() -> {
                    try {
                        logger.log(Level.INFO, "testReactiveConsumer1#kafkaTemplate.send : ");
                        kafkaTemplate.send(TOPIC_2, 1, expectedKey, expectedValue).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                })
                .assertNext(r ->
                {
                    logger.log(Level.INFO, "testReactiveConsumer1#assertNext : " + r);
                    Assertions.assertEquals(expectedValue, r);

                })
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));
    }

    @Test
    public void testReactiveConsumerShouldFail() {
        // GIVEN
        GenericFunction<String, String> handler = mock(GenericFunction.class);
        KafkaReceiver source = prepareKafkaReceiver("Test3", TOPIC_3, 2);
        Random random = new Random();
        String expectedValue = "YYY" + random.nextInt();
        String expectedKey = "KEYXXZ" + random.nextInt();
        when(handler.getMono(argThat(new ReceiverRecordMatcher<>(expectedKey, expectedValue)))).thenThrow(Exceptions.propagate(new Exception("INVALID")));

        // WHEN
        Flux<String> stream = tested.testedPipeline(source.receive(), handler, MAX_ATTEMPTS, MAX_DELAY_IN_SECONDS, RecoverableException.class);

        StepVerifier.create(stream)
                .then(() -> {
                    try {
                        logger.log(Level.INFO, "testReactiveConsumerShouldFail#kafkaTemplate.send : ");
                        kafkaTemplate.send(TOPIC_3, 2, expectedKey, expectedValue).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                })
                .thenAwait(Duration.ofSeconds(MAX_DELAY_IN_SECONDS))
//                .expectNoEvent(Duration.ofSeconds(MAX_DELAY_IN_SECONDS))
//                .expectError()
                // the KafkaReceiver will never complete, we need to cancel explicitly
                .thenCancel()
                // always use a timeout, in case we don't receive anything
                .verify(Duration.ofSeconds(15));

        Mockito.verify(handler, Mockito.times(1)).getMono(argThat(new ReceiverRecordMatcher<>(expectedKey, expectedValue)));
    }

    private static class ReceiverRecordMatcher<K, V> implements ArgumentMatcher<ReceiverRecord<K, V>> {
        private final K key;
        private final V value;

        public ReceiverRecordMatcher(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean matches(ReceiverRecord<K, V> rr) {
            if (rr == null) {
                return false;
            }
            return Objects.equals(key, rr.key()) && Objects.equals(value, rr.value());
        }
    }

    private static class RecoverableException extends Exception
    {

    }
}
