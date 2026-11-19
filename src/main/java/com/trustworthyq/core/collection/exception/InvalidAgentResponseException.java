package com.trustworthyq.core.collection.exception;

public class InvalidAgentResponseException extends RuntimeException {
    public InvalidAgentResponseException() {
        super("Invalid agent response envelope");
    }
}
