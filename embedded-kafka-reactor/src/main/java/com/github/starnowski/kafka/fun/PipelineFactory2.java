package com.github.starnowski.kafka.fun;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import static java.time.Duration.ofSeconds;

public class PipelineFactory2 {

    public <K, V, T extends Throwable> Flux<V> testedPipeline(Flux<ReceiverRecord<K, V>> source, GenericFunction<K, V> handler, int maxAttempts, int delayInSeconds, Class<T>... throwables) {
        return source.flatMap(rr ->
                Mono.defer(() ->
                        Mono.just(rr)
                                .flatMap(rr1 ->
                                {
                                        return handler.getMono(rr1);
                                })).doOnSuccess(value ->
                {
                    rr.receiverOffset().acknowledge();
                })
                        .onErrorMap(throwable ->
                                new ReceiverRecordProcessingException(throwable, rr))
                        .retryWhen(Retry
                                .backoff(maxAttempts, ofSeconds(delayInSeconds))
                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new ReceiverRecordProcessingException(retrySignal.failure(), rr))
                                .filter(throwable -> RecoverableErrorPredicate.isErrorRecoverable(throwable, 10, throwables))
                                .transientErrors(true))
        )
                .doOnError(throwable ->
                {
                    System.out.println("doOnError: " + throwable);
                    if (throwable instanceof ReceiverRecordProcessingException) {
                        ReceiverRecordProcessingException receiverRecordProcessingException = (ReceiverRecordProcessingException) throwable;
                        receiverRecordProcessingException.getReceiverRecord().receiverOffset().acknowledge();
                    }
                })
                .onErrorContinue((throwable, o) ->
                {
                    System.out.println("onErrorContinue: " + throwable);
                    if (throwable instanceof ReceiverRecordProcessingException) {
                        ReceiverRecordProcessingException receiverRecordProcessingException = (ReceiverRecordProcessingException) throwable;
                        receiverRecordProcessingException.getReceiverRecord().receiverOffset().acknowledge();
                    }
                })
                .log();
    }
}
