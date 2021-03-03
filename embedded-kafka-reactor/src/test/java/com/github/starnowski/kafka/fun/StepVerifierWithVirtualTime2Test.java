package com.github.starnowski.kafka.fun;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;

import static java.time.Duration.ofSeconds;
import static org.mockito.Mockito.*;

public class StepVerifierWithVirtualTime2Test {


    private static <T> T mockWithMockedToString(Class<T> classToMock, String message) {
        T mock = mock(classToMock);
        when(mock.toString()).thenReturn(message);
        return mock;
    }


    @Test
    public void shouldProcessAllEventsEvenWhenFirstAttemptForFirstEventFails() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class, "record2");
        ReceiverOffset receiverOffset1 = mockReceiverOffset(receiverRecord1);
        ReceiverOffset receiverOffset2 = mockReceiverOffset(receiverRecord2);
        when(supplierWithFailerHandler.getMono(receiverRecord1)).thenThrow(new RuntimeException("1234")).thenReturn(Mono.just(13));
        when(supplierWithFailerHandler.getMono(receiverRecord2)).thenReturn(Mono.just(45));


        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr ->

                                Mono.defer(() ->
                                {
                                    try {
                                        return supplierWithFailerHandler.getMono(rr);
                                    } catch (Exception ex) {
                                        throw Exceptions.propagate(ex);
                                    }
                                }).doOnSuccess(integer ->
                                {
                                    rr.receiverOffset().acknowledge();
                                })
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
        verify(supplierWithFailerHandler, times(2)).getMono(receiverRecord1);
        verify(supplierWithFailerHandler, times(1)).getMono(receiverRecord2);
        verify(receiverOffset1, times(1)).acknowledge();
        verify(receiverOffset2, times(1)).acknowledge();
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


    private ReceiverOffset mockReceiverOffset(ReceiverRecord receiverRecord) {
        ReceiverOffset receiverOffset = mock(ReceiverOffset.class);
        when(receiverRecord.receiverOffset()).thenReturn(receiverOffset);
        return receiverOffset;
    }

    private static final class SomeNonRecoverableException extends RuntimeException {
    }

    private static final class SomeRecoverableException extends RuntimeException {
    }
}
