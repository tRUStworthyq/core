package com.trustworthyq.core.chat.service;

import com.trustworthyq.core.chat.dto.CursorPage;
import com.trustworthyq.core.chat.dto.MessageResponse;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface MessageService {
    Mono<MessageResponse> sendMessage(UUID ownerId, UUID chatId, String content);
    Mono<MessageResponse> editMessage(UUID ownerId, UUID chatId, UUID messageId, String newContent);
    Mono<Void> deleteMessage(UUID ownerId, UUID chatId, UUID messageId);
    Mono<CursorPage<MessageResponse>> listMessages(UUID ownerId, UUID chatId, String cursor, Integer limit);
}
