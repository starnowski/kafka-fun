package com.github.starnowski.kafka.fun;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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

    private static boolean isErrorRecoverable(Throwable throwable, int depth)
    {
        if (throwable == null)
        {
            return false;
        }
        if (depth < 0)
        {
            return false;
        }
        if (SomeRecoverableException.class.isAssignableFrom(throwable.getClass()))
        {
            return true;
        }
        return isErrorRecoverable(throwable.getCause(), depth - 1);
    }

    @Test
    @Disabled("OnError not yet implemented")
    public void shouldProcessStreamWhenFirstFirstEventFailsWithNonRecoverableExceptionAndSecondEventFailsWithRecoverableAndStreamHasSpecifiedErrorFilter() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        ReceiverRecord<String, String> receiverRecord3 = mockWithMockedToString(ReceiverRecord.class, "record3");
        when(supplierWithFailerHandler.getMono(receiverRecord1)).thenThrow(new SomeNonRecoverableException());
        when(supplierWithFailerHandler.getMono(receiverRecord2)).thenThrow(new SomeRecoverableException());
        when(supplierWithFailerHandler.getMono(receiverRecord3)).thenReturn(Mono.just(97));
        ReceiverOffset receiverOffset1 = mockReceiverOffset(receiverRecord1);
        ReceiverOffset receiverOffset2 = mockReceiverOffset(receiverRecord2);
        ReceiverOffset receiverOffset3 = mockReceiverOffset(receiverRecord3);

        Retry retry = Retry
                .backoff(7, ofSeconds(2))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new RuntimeException("AAA"))
                .filter(throwable -> isErrorRecoverable(throwable, 10))
                .transientErrors(true);


        // WHEN
        Flux<Integer> stream = Flux.just(receiverRecord1, receiverRecord2, receiverRecord3).flatMap(rr ->
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
                }).retryWhen(retry)
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
                .expectNext(97)
                .thenAwait(ofSeconds(360))// >= (2 * 2 ^ 1) + (2 * 2 ^ 2) + (2 * 2 ^ 3) + (2 * 2 ^ 4) + (2 * 2 ^ 5) + (2 * 2 ^ 6) + (2 * 2 ^ 7) [s]
                .thenCancel()
                .verify(Duration.ofSeconds(1));
        verify(supplierWithFailerHandler, times(1)).getMono(receiverRecord1);
        verify(supplierWithFailerHandler, times(8)).getMono(receiverRecord2);
        verify(supplierWithFailerHandler, times(1)).getMono(receiverRecord3);

        verify(receiverOffset1, times(1)).acknowledge();
        verify(receiverOffset2, times(1)).acknowledge();
        verify(receiverOffset3, times(1)).acknowledge();
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
