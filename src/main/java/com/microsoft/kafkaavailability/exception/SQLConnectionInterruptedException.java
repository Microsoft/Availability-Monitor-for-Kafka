package com.microsoft.kafkaavailability.exception;

public class SQLConnectionInterruptedException extends RuntimeException {
    public SQLConnectionInterruptedException(String msg) {
        super(msg);
    }
}
