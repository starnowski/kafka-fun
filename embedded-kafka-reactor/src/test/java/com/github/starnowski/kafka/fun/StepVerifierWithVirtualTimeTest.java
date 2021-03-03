package com.github.starnowski.kafka.fun;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class StepVerifierWithVirtualTimeTest {


    private static <T> T mockWithMockedToString(Class<T> classToMock, String message) {
        T mock = mock(classToMock);
        when(mock.toString()).thenReturn(message);
        return mock;
    }

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
        verify(supplierWithFailerHandler, times(1)).get(receiverRecord1);
        verify(supplierWithFailerHandler, times(1)).get(receiverRecord2);
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
        verify(supplierWithFailerHandler, times(1)).get(receiverRecord2);
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
                                .onErrorContinue(throwable -> false, (throwable, o) -> {
                                })
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
        verify(supplierWithFailerHandler, times(1)).get(receiverRecord2);
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
                }, -1)
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
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord2);
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
                        (throwable, o) -> {
                        })
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
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord2);
        // Verify order
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord2);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
    }

    @Test
    public void shouldProcessStreamWhenFirstFirstEventFailsForAllMultipleAttempts() {
        // GIVEN
        RandomFacade randomFacade = mock(RandomFacade.class);
        RandomNumberSupplierWithFailerHandler supplierWithFailerHandler = new RandomNumberSupplierWithFailerHandler(randomFacade);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        when(randomFacade.returnNextIntForRecord(receiverRecord1)).thenThrow(new RuntimeException("1234"));
        when(randomFacade.returnNextIntForRecord(receiverRecord2)).thenReturn(73);
        InOrder inOrder = inOrder(randomFacade);
        Retry retry = Retry
                .backoff(7, ofSeconds(2))
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
                        (throwable, o) -> {
                        })
                .log()
        )
                .log();


        // THEN
        StepVerifier
                .withVirtualTime(() -> stream)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .expectNext(73)
                .thenAwait(ofSeconds(360))//TODO check how min and max backoff is calculated
                .thenCancel()
                .verify(Duration.ofSeconds(1));
        ;
        verify(randomFacade, times(8)).returnNextIntForRecord(receiverRecord1);
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord2);
        // Verify order
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord2);
        inOrder.verify(randomFacade, times(7)).returnNextIntForRecord(receiverRecord1);
    }

    @Test
    public void shouldProcessStreamWhenFirstFirstEventFailsForAllMultipleAttemptsWithSpecifiedErrorFilter() {
        // GIVEN
        RandomFacade randomFacade = mock(RandomFacade.class);
        RandomNumberSupplierWithFailerHandler supplierWithFailerHandler = new RandomNumberSupplierWithFailerHandler(randomFacade);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        when(randomFacade.returnNextIntForRecord(receiverRecord1)).thenThrow(new SomeNonRecoverableException());
        when(randomFacade.returnNextIntForRecord(receiverRecord2)).thenReturn(73);
        InOrder inOrder = inOrder(randomFacade);
        Retry retry = Retry
                .backoff(7, ofSeconds(2))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new RuntimeException("AAA"))
                .filter(throwable -> SomeNonRecoverableException.class.equals(throwable.getClass()))
                .transientErrors(true);


        // WHEN
        Flux<Integer> stream = Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr).retryWhen(retry)
                .log()
                .onErrorContinue(throwable ->
                        {
                            System.out.println("Error in stream: " + throwable);
                            return true;
                        },
                        (throwable, o) -> {
                        })
                .log()
        )
                .log();


        // THEN
        StepVerifier
                .withVirtualTime(() -> stream)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(ofSeconds(2))
                .expectNext(73)
                .thenAwait(ofSeconds(360))//TODO check how min and max backoff is calculated
                .thenCancel()
                .verify(Duration.ofSeconds(1));
        verify(randomFacade, times(8)).returnNextIntForRecord(receiverRecord1);
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord2);
        // Verify order
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord2);
        inOrder.verify(randomFacade, times(7)).returnNextIntForRecord(receiverRecord1);
    }

    @Test
    public void shouldProcessStreamWhenFirstFirstEventFailsAndNotRetryWhenSpecifiedErrorFilterIgnoreThrownException() {
        // GIVEN
        RandomFacade randomFacade = mock(RandomFacade.class);
        RandomNumberSupplierWithFailerHandler supplierWithFailerHandler = new RandomNumberSupplierWithFailerHandler(randomFacade);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        when(randomFacade.returnNextIntForRecord(receiverRecord1)).thenThrow(new SomeNonRecoverableException());
        when(randomFacade.returnNextIntForRecord(receiverRecord2)).thenReturn(89);
        InOrder inOrder = inOrder(randomFacade);
        Retry retry = Retry
                .backoff(7, ofSeconds(2))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new RuntimeException("AAA"))
                .filter(throwable -> !SomeNonRecoverableException.class.equals(throwable.getClass()))
                .transientErrors(true);


        // WHEN
        Flux<Integer> stream = Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr).retryWhen(retry)
                .log()
                .onErrorContinue(throwable ->
                        {
                            System.out.println("Error in stream: " + throwable);
                            return true;
                        },
                        (throwable, o) -> {
                        })
                .log()
        )
                .log();


        // THEN
        StepVerifier
                .withVirtualTime(() -> stream)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .expectNext(89)
                .thenCancel()
                .verify(Duration.ofSeconds(1));
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord1);
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord2);
        // Verify order
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord2);
    }

    @Test
    public void shouldProcessStreamWhenFirstFirstEventFailsWithNonRecoverableExceptionAndSecondEventFailsWithRecoverableAndStreamHasSpecifiedErrorFilter() {
        // GIVEN
        RandomFacade randomFacade = mock(RandomFacade.class);
        RandomNumberSupplierWithFailerHandler supplierWithFailerHandler = new RandomNumberSupplierWithFailerHandler(randomFacade);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        ReceiverRecord<String, String> receiverRecord3 = mockWithMockedToString(ReceiverRecord.class, "record2");
        when(randomFacade.returnNextIntForRecord(receiverRecord1)).thenThrow(new SomeNonRecoverableException());
        when(randomFacade.returnNextIntForRecord(receiverRecord2)).thenThrow(new SomeRecoverableException());
        when(randomFacade.returnNextIntForRecord(receiverRecord3)).thenReturn(17);
        InOrder inOrder = inOrder(randomFacade);
        Retry retry = Retry
                .backoff(7, ofSeconds(2))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new RuntimeException("AAA"))
                .filter(throwable -> !SomeNonRecoverableException.class.equals(throwable.getClass()))
                .transientErrors(true);


        // WHEN
        Flux<Integer> stream = Flux.just(receiverRecord1, receiverRecord2, receiverRecord3).flatMap(rr -> supplierWithFailerHandler.get(rr).retryWhen(retry)
                .log()
                .onErrorContinue(throwable ->
                        {
                            System.out.println("Error in stream: " + throwable);
                            return true;
                        },
                        (throwable, o) -> {
                        })
                .log()
        )
                .log();


        // THEN
        StepVerifier
                .withVirtualTime(() -> stream)
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .expectNoEvent(Duration.ofSeconds(2))
                .expectNext(17)
                .thenAwait(ofSeconds(360))// >= (2 * 2 ^ 1) + (2 * 2 ^ 2) + (2 * 2 ^ 3) + (2 * 2 ^ 4) + (2 * 2 ^ 5) + (2 * 2 ^ 6) + (2 * 2 ^ 7) [s]
                .thenCancel()
                .verify(Duration.ofSeconds(1));
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord1);
        verify(randomFacade, times(8)).returnNextIntForRecord(receiverRecord2);
        verify(randomFacade, times(1)).returnNextIntForRecord(receiverRecord3);
        // Verify order
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord1);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord2);
        inOrder.verify(randomFacade).returnNextIntForRecord(receiverRecord3);
        inOrder.verify(randomFacade, times(7)).returnNextIntForRecord(receiverRecord2);
    }

//    @Test
//    public void shouldProcessAllEventsEvenWhenFirstAttemptForFirstEventFailsAndXXXXXXXXX() {
//        // GIVEN
//        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
//        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class);
//        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class);
//        when(supplierWithFailerHandler.get(receiverRecord1)).thenThrow(new RuntimeException("1234")).thenReturn(Flux.just(13));
//        when(supplierWithFailerHandler.get(receiverRecord2)).thenReturn(Flux.just(45));
//
//
//        // THEN
//        StepVerifier
//                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr)
//                        )
//                                .log()
//                                .retryWhen(
//                                        Retry
//                                                .backoff(1, ofSeconds(2))
//                                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
//
//                                                    return new RuntimeException("AAA");
//                                                })
//                                                .transientErrors(true)
//                                )
//                                .log()
//                )
//                .expectSubscription()
//                .expectNoEvent(Duration.ofSeconds(2))
//                .thenAwait(ofSeconds(2))
//                .thenAwait(ofSeconds(2))
//                .expectNext(13)
//                .expectNext(45)
//                .verifyComplete();
//        verify(supplierWithFailerHandler, times(2)).get(receiverRecord1);
//        verify(supplierWithFailerHandler, times(1)).get(receiverRecord2);
//
//
//    }

    private ReceiverOffset mockReceiverOffset(ReceiverRecord receiverRecord)
    {
        ReceiverOffset receiverOffset = mock(ReceiverOffset.class);
        when(receiverRecord.receiverOffset()).thenReturn(receiverOffset);
        return receiverOffset;
    }

    private static final class SomeNonRecoverableException extends RuntimeException {
    }

    private static final class SomeRecoverableException extends RuntimeException {
    }
}
