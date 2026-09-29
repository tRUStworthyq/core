package com.trustworthyq.core.chat.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateChatTitleRequest(
        @NotBlank(message = "Название чата не может быть пустым")
        @Size(max = 255, message = "Название чата слишком длинное")
        String title
) {
}