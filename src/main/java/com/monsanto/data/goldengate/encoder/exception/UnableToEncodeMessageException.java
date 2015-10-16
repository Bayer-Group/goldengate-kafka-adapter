package com.monsanto.data.goldengate.encoder.exception;

public class UnableToEncodeMessageException extends RuntimeException {
    public UnableToEncodeMessageException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
