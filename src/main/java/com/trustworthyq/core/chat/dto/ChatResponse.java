package com.trustworthyq.core.chat.dto;

import java.time.Instant;
import java.util.UUID;
import com.trustworthyq.core.chat.entity.ChatEntity;

public record ChatResponse(UUID id, String title, Instant createdAt, Instant updatedAt) {
    public static ChatResponse from(ChatEntity chat) {
        return new ChatResponse(chat.getId(), chat.getTitle(), chat.getCreatedAt(), chat.getUpdatedAt());
    }
}