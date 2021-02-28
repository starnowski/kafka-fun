package com.github.starnowski.kafka.fun;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class StepVerifierWithVirtualTimeTest {


    @Test
    public void shouldCompleteAfterFirstAttemptFailedWithVirtualTime() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = new ConstantNumberSupplierWithFailerHandler(2);
        ReceiverRecord<String, String> receiverRecord = mock(ReceiverRecord.class);

        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord)
                        .flatMap(rr -> supplierWithFailerHandler.get(rr))
                        .log()
                        .retryWhen(Retry.backoff(1, ofSeconds(2)))
                        .log()
                )
                .expectSubscription()
                .expectNoEvent(ofSeconds(2))
                .thenAwait(ofSeconds(1))
                .expectNext(13)
                .verifyComplete();

        assertEquals(2, supplierWithFailerHandler.getCurrent());
    }

    @Test
    public void shouldThrowExceptionAfterMaxRetiresWithVirtualTime() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = new ConstantNumberSupplierWithFailerHandler(12);
        ReceiverRecord<String, String> receiverRecord = mock(ReceiverRecord.class);

        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord).flatMap(rr -> supplierWithFailerHandler.get(rr)
                        )
                                .retryWhen(Retry.backoff(1, ofSeconds(2)))
                        .log()
                )
                .expectSubscription()
                .expectNoEvent(ofSeconds(2))
                .thenAwait(ofSeconds(1))
                .verifyError(RuntimeException.class);
        assertEquals(2, supplierWithFailerHandler.getCurrent());
    }

    @Test
    public void shouldProcessAllEventsWhenReturnCorrectValues() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class);
        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class);
        when(supplierWithFailerHandler.get(receiverRecord1)).thenReturn(Flux.just(13));
        when(supplierWithFailerHandler.get(receiverRecord2)).thenReturn(Flux.just(45));


        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr)
                        )
                        .log()
                                .retryWhen(
                                        Retry
                                                .backoff(1, ofSeconds(2))
                                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {

                                                    return new RuntimeException("AAA");
                                                })
                                                .transientErrors(true)
                                )
                                .log()
                )
                .expectSubscription()
                .expectNext(13)
                .expectNext(45)
                .verifyComplete();
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord1);
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord2);
    }

    @Test
    public void shouldProcessAllEventsEvenWhenFirstAttemptForFirstEventFails() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class);
        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class);
        when(supplierWithFailerHandler.get(receiverRecord1)).thenThrow(new RuntimeException("1234")).thenReturn(Flux.just(13));
        when(supplierWithFailerHandler.get(receiverRecord2)).thenReturn(Flux.just(45));


        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr)
                        )
                                .log()
                                .retryWhen(
                                        Retry
                                                .backoff(1, ofSeconds(2))
                                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {

                                                    return new RuntimeException("AAA");
                                                })
                                                .transientErrors(true)
                                )
                                .log()
                )
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .expectNext(13)
                .expectNext(45)
                .verifyComplete();
        verify(supplierWithFailerHandler, times(2)).get(receiverRecord1);
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord2);
    }

    @Test
    public void shouldProcessAllEventsEvenWhenFirstAttemptForFirstEventFailsWithRedundantDeclarationOfOnErrorContinue() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class);
        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class);
        when(supplierWithFailerHandler.get(receiverRecord1)).thenThrow(new RuntimeException("1234")).thenReturn(Flux.just(13));
        when(supplierWithFailerHandler.get(receiverRecord2)).thenReturn(Flux.just(45));


        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr)
                        )
                                .log()
                                .retryWhen(
                                        Retry
                                                .backoff(1, ofSeconds(2))
                                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {

                                                    return new RuntimeException("AAA");
                                                })
                                                .transientErrors(true)
                                )
                                // Redundant declaration
                                .onErrorContinue(throwable -> false, (throwable, o) -> {})
                                .log()
                )
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .expectNext(13)
                .expectNext(45)
                .verifyComplete();
        verify(supplierWithFailerHandler, times(2)).get(receiverRecord1);
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord2);
    }

    @Test
    public void shouldProcessStreamAndReturnFakeValueForFirstEventWhenFirstFirstEventFailsForAllAttempts() {
        // GIVEN
        RandomFacade randomFacade = mock(RandomFacade.class);
        RandomNumberSupplierWithFailerHandler supplierWithFailerHandler = new RandomNumberSupplierWithFailerHandler(randomFacade);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        when(randomFacade.returnNextIntForRecord(receiverRecord1)).thenThrow(new RuntimeException("1234"));
        when(randomFacade.returnNextIntForRecord(receiverRecord2)).thenReturn(45);
        Retry retry = Retry
                .backoff(1, ofSeconds(2))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new RuntimeException("AAA"))
                .transientErrors(true);


        // WHEN
        Flux<Integer> stream = Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr).retryWhen(retry)
                .log()
                .onErrorReturn(throwable ->
                {
                    System.out.println("Error in stream: " + throwable);
                    return true;
                },-1)
                .log()
        )
                .log();


        // THEN
        StepVerifier
                .withVirtualTime(() -> stream)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .expectNext(45)
                .expectNext(-1)
                .verifyComplete()
        ;
        verify(randomFacade, times(2)).returnNextIntForRecord(receiverRecord1);
        verify(randomFacade, atMostOnce()).returnNextIntForRecord(receiverRecord2);
    }

    @Test
    public void shouldProcessStreamWhenFirstFirstEventFailsForAllAttempts() {
        // GIVEN
        RandomFacade randomFacade = mock(RandomFacade.class);
        RandomNumberSupplierWithFailerHandler supplierWithFailerHandler = new RandomNumberSupplierWithFailerHandler(randomFacade);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        when(randomFacade.returnNextIntForRecord(receiverRecord1)).thenThrow(new RuntimeException("1234"));
        when(randomFacade.returnNextIntForRecord(receiverRecord2)).thenReturn(45);
        InOrder inOrder = inOrder(randomFacade);
        Retry retry = Retry
                .backoff(1, ofSeconds(2))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new RuntimeException("AAA"))
                .transientErrors(true);


        // WHEN
        Flux<Integer> stream = Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr).retryWhen(retry)
                .log()
                .onErrorContinue(throwable ->
                {
                    System.out.println("Error in stream: " + throwable);
                    return true;
                },
                        (throwable, o) -> {})
                .log()
        )
                .log();


        // THEN
        StepVerifier
                .withVirtualTime(() -> stream)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .expectNext(45)
                .thenAwait(ofSeconds(2))
                .thenCancel()
                .verify(Duration.ofSeconds(1));
        ;
        verify(randomFacade, times(2)).returnNextIntForRecord(receiverRecord1);
        verify(randomFacade, atMostOnce()).returnNextIntForRecord(receiverRecord2);
        // Verify order
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord2);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
    }

    private static <T> T mockWithMockedToString(Class<T> classToMock, String message) {
        T mock = mock(classToMock);
        when(mock.toString()).thenReturn(message);
        return mock;
    }

    private static final class RetryFailedException extends RuntimeException
    {}
}
