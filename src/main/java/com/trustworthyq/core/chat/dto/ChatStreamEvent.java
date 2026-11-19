package com.trustworthyq.core.chat.dto;

import java.util.UUID;

public record ChatStreamEvent(String type, String text, UUID chatId) {
    public static ChatStreamEvent phase(String text) {
        return new ChatStreamEvent("phase", text, null);
    }

    public static ChatStreamEvent delta(String text) {
        return new ChatStreamEvent("delta", text, null);
    }

    public static ChatStreamEvent done(UUID chatId) {
        return new ChatStreamEvent("done", null, chatId);
    }

    public static ChatStreamEvent error(String text) {
        return new ChatStreamEvent("error", text, null);
    }
}
