package com.monsanto.data.goldengate.kafka;

public class UnableToSendMessageException extends RuntimeException {
    public UnableToSendMessageException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
