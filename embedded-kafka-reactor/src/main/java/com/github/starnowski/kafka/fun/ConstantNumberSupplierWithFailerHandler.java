package com.github.starnowski.kafka.fun;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public class ConstantNumberSupplierWithFailerHandler {

    private final int maxAttempt;
    private int current = 0;

    public ConstantNumberSupplierWithFailerHandler(int maxAttempt) {
        this.maxAttempt = maxAttempt;
    }

    public int getCurrent() {
        return current;
    }

    public Flux<Integer> get(ReceiverRecord<String, String> receiverRecord) {
        current++;
        if (current < maxAttempt) {
            throw new RuntimeException("xxx");
        }
        return Flux.just(13);
    }

    public Mono<Integer> getMono(ReceiverRecord<String, String> receiverRecord) {
        current++;
        if (current < maxAttempt) {
            throw new RuntimeException("xxx");
        }
        return Mono.just(13);
    }
}
