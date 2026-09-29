package com.trustworthyq.core.chat.dto;

import java.time.Instant;
import java.util.UUID;
import com.trustworthyq.core.chat.entity.MessageEntity;
import com.trustworthyq.core.chat.enums.MessageRole;

public record MessageResponse(UUID id, UUID chatId, MessageRole role, String content, boolean edited,
                              Instant createdAt, Instant updatedAt) {
    public static MessageResponse from(MessageEntity message) {
        return new MessageResponse(message.getId(), message.getChatId(), message.getRole(), message.getContent(),
                message.isEdited(), message.getCreatedAt(), message.getUpdatedAt());
    }
}