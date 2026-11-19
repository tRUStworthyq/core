package com.trustworthyq.core.ai.service;

import com.trustworthyq.core.chat.dto.MessageResponse;
import reactor.core.publisher.Mono;

import java.util.List;

public interface OllamaChatService {
    Mono<String> chat(List<MessageResponse> history, String prompt);
}
