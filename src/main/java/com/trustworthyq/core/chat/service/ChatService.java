package com.trustworthyq.core.chat.service;

import com.trustworthyq.core.chat.dto.ChatResponse;
import com.trustworthyq.core.chat.dto.CursorPage;
import org.springframework.ai.ollama.api.OllamaApi;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface ChatService {
    Mono<ChatResponse> createChat(UUID ownerId, String title);
    Mono<ChatResponse> getChat(UUID ownerId, UUID chatId);
    Mono<Void> deleteChat(UUID ownerId, UUID chatId);
    Mono<CursorPage<ChatResponse>> listChats(UUID ownerId, String cursor, Integer limit);
}
