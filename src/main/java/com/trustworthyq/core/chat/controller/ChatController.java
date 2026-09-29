package com.trustworthyq.core.chat.controller;

import com.trustworthyq.core.chat.dto.ChatResponse;
import com.trustworthyq.core.chat.dto.CreateChatRequest;
import com.trustworthyq.core.chat.dto.CreateChatTitleRequest;
import com.trustworthyq.core.chat.dto.CursorPage;
import com.trustworthyq.core.chat.service.ChatService;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/chats")
@RequiredArgsConstructor
public class ChatController {

    private final ChatService chatService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ChatResponse> createChat(@RequestHeader("X-User-Id") UUID ownerId,
                                         @Valid @RequestBody CreateChatRequest request) {
        return chatService.createChat(ownerId, request.content());
    }

    @GetMapping
    public Mono<CursorPage<ChatResponse>> listChats(@RequestHeader("X-User-Id") UUID ownerId,
                                                    @RequestParam(required = false) String cursor,
                                                    @RequestParam(required = false) Integer limit) {
        return chatService.listChats(ownerId, cursor, limit);
    }

    @GetMapping("/{chatId}")
    public Mono<ChatResponse> getChat(@RequestHeader("X-User-Id") UUID ownerId, @PathVariable UUID chatId) {
        return chatService.getChat(ownerId, chatId);
    }

    @DeleteMapping("/{chatId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteChat(@RequestHeader("X-User-Id") UUID ownerId, @PathVariable UUID chatId) {
        return chatService.deleteChat(ownerId, chatId);
    }
}