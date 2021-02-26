package com.github.starnowski.kafka.fun;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;

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
                        .retryWhen(Retry.backoff(1, Duration.ofSeconds(2)))
                )
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(Duration.ofSeconds(1))
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
                                .retryWhen(Retry.backoff(1, Duration.ofSeconds(2)))
                )
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(Duration.ofSeconds(1))
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
                                .retryWhen(
                                        Retry
                                                .backoff(1, Duration.ofSeconds(2))
                                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {

                                                    return new RuntimeException("AAA");
                                                })
                                                .transientErrors(true)
                                )
                )
                .expectSubscription()
                .expectNext(13)
                .expectNext(45)
                .verifyComplete();
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord1);
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord2);
    }

    @Test
    @Disabled("Not yet finished")
    public void xxx() {
        // GIVEN
        ConstantNumberSupplierWithFailerHandler supplierWithFailerHandler = mock(ConstantNumberSupplierWithFailerHandler.class);
        ReceiverRecord<String, String> receiverRecord1 = mock(ReceiverRecord.class);
        ReceiverRecord<String, String> receiverRecord2 = mock(ReceiverRecord.class);
        when(supplierWithFailerHandler.get(receiverRecord1)).thenThrow(new RuntimeException("1234"));
        when(supplierWithFailerHandler.get(receiverRecord2)).thenReturn(Flux.just(45));


        // THEN
        StepVerifier
                .withVirtualTime(() -> Flux.just(receiverRecord1, receiverRecord2).flatMap(rr -> supplierWithFailerHandler.get(rr)
//                                .retryWhen(Retry.backoff(1, Duration.ofSeconds(8)))
                        )
                        .retryWhen(
                                Retry
                                        .backoff(1, Duration.ofSeconds(2))
                                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {

                                            return new RuntimeException("AAA");
                                        })
                                        .transientErrors(true)
                        )
//                                .onErrorContinue(throwable -> {
//                                    System.out.println(throwable);
//                                    return RuntimeException.class.equals(throwable.getClass()) && "AAA".equals(throwable.getMessage());
//                                }, (throwable, o) -> {
//
//                                })
                                .onErrorReturn(throwable -> {
                                            System.out.println(throwable);
                                            return RuntimeException.class.equals(throwable.getClass()) && "AAA".equals(throwable.getMessage());
                                        },
                                        -1
                                        )
                )
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(2))
                .thenAwait(Duration.ofSeconds(2))
                .expectNext(-1)
                .expectNext(45)
                .verifyComplete();
        verify(supplierWithFailerHandler, times(2)).get(receiverRecord1);
        verify(supplierWithFailerHandler, atMostOnce()).get(receiverRecord2);
    }
}
