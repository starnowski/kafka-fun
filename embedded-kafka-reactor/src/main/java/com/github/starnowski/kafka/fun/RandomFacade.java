package com.github.starnowski.kafka.fun;

import reactor.kafka.receiver.ReceiverRecord;

import java.util.Random;

public class RandomFacade {

    private Random random = new Random();

    public int returnNextIntForRecord(ReceiverRecord<String, String> receiverRecord)
    {
        return random.nextInt();
    }

    public int returnNextInt()
    {
        return random.nextInt();
    }
}
