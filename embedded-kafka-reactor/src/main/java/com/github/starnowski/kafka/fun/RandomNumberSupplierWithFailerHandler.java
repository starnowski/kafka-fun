package com.github.starnowski.kafka.fun;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

public class RandomNumberSupplierWithFailerHandler {

    public RandomNumberSupplierWithFailerHandler(RandomFacade randomFacade) {
        this.randomFacade = randomFacade;
    }

    private final RandomFacade randomFacade;

    public Flux<Integer> get(ReceiverRecord<String, String> receiverRecord)
    {
        return Flux.defer(() -> Flux.just(randomFacade.returnNextIntForRecord(receiverRecord)));
    }
}
