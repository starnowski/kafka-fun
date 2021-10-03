package com.github.starnowski.kafka.fun;

import reactor.kafka.receiver.ReceiverRecord;

public class ReceiverRecordProcessingException extends RuntimeException {
    final ReceiverRecord receiverRecord;

    public ReceiverRecordProcessingException(Throwable cause, ReceiverRecord receiverRecord) {
        super(cause);
        this.receiverRecord = receiverRecord;
    }

    public ReceiverRecord getReceiverRecord() {
        return receiverRecord;
    }
}