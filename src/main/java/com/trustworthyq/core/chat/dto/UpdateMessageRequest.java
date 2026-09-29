package com.trustworthyq.core.chat.dto;

import jakarta.validation.constraints.NotBlank;

public record UpdateMessageRequest(@NotBlank(message = "Сообщение не может быть пустым") String content) {
}