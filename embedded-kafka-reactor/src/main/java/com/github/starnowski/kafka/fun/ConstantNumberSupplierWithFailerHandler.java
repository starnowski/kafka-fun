package com.github.starnowski.kafka.fun;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Random;

public class ConstantNumberSupplierWithFailerHandler {

    private final int maxAttempt;

    public int getCurrent() {
        return current;
    }

    private int current = 0;

    public ConstantNumberSupplierWithFailerHandler(int maxAttempt) {
        this.maxAttempt = maxAttempt;
    }

    public Flux<Integer> get(ReceiverRecord<String, String> receiverRecord)
    {
        current++;
        if (current < maxAttempt)
        {
            throw new RuntimeException("xxx");
        }
        return Flux.just(13);
    }
}
