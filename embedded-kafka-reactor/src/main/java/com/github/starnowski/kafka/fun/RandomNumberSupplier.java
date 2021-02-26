package com.github.starnowski.kafka.fun;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Random;

public class RandomNumberSupplier {

    public Flux<Integer> get(ReceiverRecord<String, String> receiverRecord)
    {
        Random r = new Random();
        return Flux.just(r.nextInt());
    }
}
