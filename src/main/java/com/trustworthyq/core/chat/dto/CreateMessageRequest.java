package com.trustworthyq.core.chat.dto;

import jakarta.validation.constraints.NotBlank;
import java.util.UUID;

public record CreateMessageRequest(@NotBlank(message = "Сообщение не может быть пустым") String content, UUID requestId) {
}