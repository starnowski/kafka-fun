package com.github.starnowski.kafka.fun;

import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public class GenericFunction<K, V> {

    public Mono<V> getMono(ReceiverRecord<K, V> receiverRecord) {
        return Mono.just(receiverRecord.value());
    }
}
