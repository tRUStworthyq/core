package com.trustworthyq.core.chat.exception;

import java.util.UUID;

public class ChatNotFoundException extends RuntimeException {
    public ChatNotFoundException(UUID chatId) {
        super("Чат не найден: " + chatId);
    }
}