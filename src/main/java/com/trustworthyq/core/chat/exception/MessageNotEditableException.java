package com.trustworthyq.core.chat.exception;

import java.util.UUID;

public class MessageNotEditableException extends RuntimeException {
    public MessageNotEditableException(UUID messageId) {
        super("Сообщение не обновляемо: " + messageId);
    }
}
