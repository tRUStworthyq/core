package com.trustworthyq.core.chat.exception;

import java.util.UUID;

public class MessageNotFoundException extends RuntimeException {
    public MessageNotFoundException(UUID messageId) {
        super("Сообщение не найдено: " + messageId);
    }
}