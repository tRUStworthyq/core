package com.trustworthyq.core.chat.dto;

import java.time.Instant;
import java.util.UUID;
import com.trustworthyq.core.chat.entity.MessageEntity;

public record MessageResponse(UUID id, UUID chatId, String content, boolean edited,
                              Instant createdAt, Instant updatedAt) {
    public static MessageResponse from(MessageEntity message) {
        return new MessageResponse(message.getId(), message.getChatId(), message.getContent(),
                message.isEdited(), message.getCreatedAt(), message.getUpdatedAt());
    }
}