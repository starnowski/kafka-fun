package com.github.starnowski.kafka.fun;

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

public class StepVerifierWithVirtualTime3Test {


    private static Flux<Integer> testedPipeline(Flux<ReceiverRecord<String, String>> source, ConstantNumberSupplierWithFailerHandler handler) {
        return source.flatMap(rr ->
                Mono.defer(() ->
                {
                    try {
                        return handler.getMono(rr);
                    } catch (Exception ex) {
                        /*
                         * This is not good practice (it is better to return Mono.error(ex)) but we want to simulate a
                         * case when a component from the lower layer does not stick to this rule.
                         */
                        throw Exceptions.propagate(ex);
                    }
                }).doOnSuccess(integer ->
                {
                    rr.receiverOffset().acknowledge();
                })
                        .onErrorMap(throwable ->
                        {
                            return new ReceiverRecordProcessingException(throwable, rr);
                        })
                        .retryWhen(Retry
                                .backoff(7, ofSeconds(2))
                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new ReceiverRecordProcessingException(retrySignal.failure(), rr))
                                .filter(throwable -> isErrorRecoverable(throwable, 10))
                                .transientErrors(true))
                        .log()
                        .onErrorContinue(throwable ->
                                {
                                    System.out.println("Error in stream: " + throwable);
                                    return true;
                                },
                                (throwable, o) -> {
                                    if (throwable instanceof ReceiverRecordProcessingException) {
                                        ReceiverRecordProcessingException exception = (ReceiverRecordProcessingException) throwable;
                                        exception.getReceiverRecord().receiverOffset().acknowledge();
                                    }
                                })
                        .log()
        )
                .log();
    }

    private static <T> T mockWithMockedToString(Class<T> classToMock, String message) {
        T mock = mock(classToMock);
        when(mock.toString()).thenReturn(message);
        return mock;
    }

    private static boolean isErrorRecoverable(Throwable throwable, int depth) {
        if (throwable == null) {
            return false;
        }
        if (depth < 0) {
            return false;
        }
        if (SomeRecoverableException.class.isAssignableFrom(throwable.getClass())) {
            return true;
        }
        return isErrorRecoverable(throwable.getCause(), depth - 1);
    }

//    @Test
//    public void shouldProcessAllEventsEvenWhenFirstAttemptForFirstEventFails() {
//        // GIVEN
//        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
//        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class, "record1");
//        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class, "record2");
//        ReceiverOffset receiverOffset1 = mockReceiverOffset(receiverRecord1);
//        ReceiverOffset receiverOffset2 = mockReceiverOffset(receiverRecord2);
//        when(supplierWithFailerHandler.getMono(receiverRecord1)).thenThrow(new RuntimeException("1234")).thenReturn(Mono.just(13));
//        when(supplierWithFailerHandler.getMono(receiverRecord2)).thenReturn(Mono.just(45));
//
//
//        // THEN
//        StepVerifier
//                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr ->
//
//                                Mono.defer(() ->
//                                {
//                                    try {
//                                        return supplierWithFailerHandler.getMono(rr);
//                                    } catch (Exception ex) {
//                                        throw Exceptions.propagate(ex);
//                                    }
//                                }).doOnSuccess(integer ->
//                                {
//                                    rr.receiverOffset().acknowledge();
//                                })
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
//        verify(supplierWithFailerHandler, times(2)).getMono(receiverRecord1);
//        verify(supplierWithFailerHandler, times(1)).getMono(receiverRecord2);
//        verify(receiverOffset1, times(1)).acknowledge();
//        verify(receiverOffset2, times(1)).acknowledge();
//    }

    @Test
    public void shouldProcessStreamWhenFirstFirstEventFailsWithNonRecoverableExceptionAndSecondEventFailsWithRecoverableAndStreamHasSpecifiedErrorFilter() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler handler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mockWithMockedToString(ReceiverRecord.class, "record1");
        ReceiverRecord<String, String> receiverRecord2 = mockWithMockedToString(ReceiverRecord.class, "record2");
        ReceiverRecord<String, String> receiverRecord3 = mockWithMockedToString(ReceiverRecord.class, "record3");
        when(handler.getMono(receiverRecord1)).thenThrow(new SomeNonRecoverableException());
        when(handler.getMono(receiverRecord2)).thenThrow(new SomeRecoverableException());
        when(handler.getMono(receiverRecord3)).thenReturn(Mono.just(97));
        ReceiverOffset receiverOffset1 = mockReceiverOffset(receiverRecord1);
        ReceiverOffset receiverOffset2 = mockReceiverOffset(receiverRecord2);
        ReceiverOffset receiverOffset3 = mockReceiverOffset(receiverRecord3);

        // WHEN
        Flux<Integer> stream = testedPipeline(Flux.just(receiverRecord1, receiverRecord2, receiverRecord3), handler);


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
        verify(handler, times(1)).getMono(receiverRecord1);
        verify(handler, times(8)).getMono(receiverRecord2);
        verify(handler, times(1)).getMono(receiverRecord3);

        verify(receiverOffset1, times(1)).acknowledge();
        verify(receiverOffset2, times(1)).acknowledge();
        verify(receiverOffset3, times(1)).acknowledge();
    }

//
//    @Test
//    public void shouldNotMapErrorWhenCheckedExceptionIsPropagatedAsReactiveException() {
//        // GIVEN
//        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
//        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class, "record1");
//        ReceiverOffset receiverOffset1 = mockReceiverOffset(receiverRecord1);
//        when(supplierWithFailerHandler.getMono(receiverRecord1)).thenThrow(Exceptions.propagate(new Exception("XXX1")));
//
//
//        // THEN
//        StepVerifier
//                .withVirtualTime(() -> Flux.just(receiverRecord1).flatMap(rr ->
//
//                                Mono.defer(() ->
//                                        supplierWithFailerHandler.getMono(rr)).doOnSuccess(integer ->
//                                {
//                                    rr.receiverOffset().acknowledge();
//                                })
//                                        .onErrorMap(throwable ->
//                                        {
//                                            return new ReceiverRecordProcessingException(throwable, rr);
//                                        })
//                        )
//                                .onErrorContinue((throwable, o) ->
//                                {
//                                    if (throwable instanceof ReceiverRecordProcessingException) {
//                                        ReceiverRecordProcessingException receiverRecordProcessingException = (ReceiverRecordProcessingException) throwable;
//                                        receiverRecordProcessingException.getReceiverRecord().receiverOffset().acknowledge();
//                                    }
//                                })
//                                .log()
//                )
//                .thenCancel()
//                .verify(Duration.ofSeconds(1));
//        verify(supplierWithFailerHandler, times(1)).getMono(receiverRecord1);
//        verify(receiverOffset1, Mockito.never()).acknowledge();
//    }
//
//    @Test
//    public void shouldNMapErrorWhenCheckedExceptionIsPropagatedAsReactiveException() {
//        // GIVEN
//        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
//        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class, "record1");
//        ReceiverOffset receiverOffset1 = mockReceiverOffset(receiverRecord1);
//        when(supplierWithFailerHandler.getMono(receiverRecord1)).thenThrow(Exceptions.propagate(new Exception("XXX1")));
//
//
//        // THEN
//        StepVerifier
//                .withVirtualTime(() -> Flux.just(receiverRecord1).flatMap(rr ->
//                                Mono.defer(() ->
//                                        {
//                                            try {
//                                                return supplierWithFailerHandler.getMono(rr);
//                                            } catch (Exception ex) {
//                                                throw Exceptions.propagate(ex);
//                                            }
//                                        }
//                                ).onErrorMap(throwable -> new ReceiverRecordProcessingException(throwable, rr))
//                        )
//                                .doOnError(throwable ->
//                                {
//                                    if (throwable instanceof ReceiverRecordProcessingException) {
//                                        ReceiverRecordProcessingException receiverRecordProcessingException = (ReceiverRecordProcessingException) throwable;
//                                        receiverRecordProcessingException.getReceiverRecord().receiverOffset().acknowledge();
//                                    }
//                                })
//                                .onErrorContinue((throwable, o) ->
//                                {
//                                    if (throwable instanceof ReceiverRecordProcessingException) {
//                                        ReceiverRecordProcessingException receiverRecordProcessingException = (ReceiverRecordProcessingException) throwable;
//                                        receiverRecordProcessingException.getReceiverRecord().receiverOffset().acknowledge();
//                                    }
//                                })
//                                .log()
//                )
//                .expectSubscription()
//                .thenCancel()
//                .verify(Duration.ofSeconds(1));
//        verify(supplierWithFailerHandler, times(1)).getMono(receiverRecord1);
//        verify(receiverOffset1, times(1)).acknowledge();
//    }


    private ReceiverOffset mockReceiverOffset(ReceiverRecord receiverRecord) {
        ReceiverOffset receiverOffset = mock(ReceiverOffset.class);
        when(receiverRecord.receiverOffset()).thenReturn(receiverOffset);
        return receiverOffset;
    }

    private static final class ReceiverRecordProcessingException extends RuntimeException {
        final ReceiverRecord receiverRecord;

        public ReceiverRecordProcessingException(Throwable cause, ReceiverRecord receiverRecord) {
            super(cause);
            this.receiverRecord = receiverRecord;
        }

        public ReceiverRecord getReceiverRecord() {
            return receiverRecord;
        }
    }

    private static final class SomeNonRecoverableException extends RuntimeException {
    }

    private static final class SomeRecoverableException extends RuntimeException {
    }
}
