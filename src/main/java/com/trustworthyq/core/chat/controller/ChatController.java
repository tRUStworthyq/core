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
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/chats")
@RequiredArgsConstructor
public class ChatController {

    private final ChatService chatService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ChatResponse> createChat(@AuthenticationPrincipal Jwt jwt,
                                         @Valid @RequestBody CreateChatRequest request) {
        return chatService.createChat(UUID.fromString(jwt.getSubject()), request.content());
    }

    @GetMapping
    public Mono<CursorPage<ChatResponse>> listChats(@AuthenticationPrincipal Jwt jwt,
                                                    @RequestParam(required = false) String cursor,
                                                    @RequestParam(required = false) Integer limit) {
        return chatService.listChats(UUID.fromString(jwt.getSubject()), cursor, limit);
    }

    @GetMapping("/{chatId}")
    public Mono<ChatResponse> getChat(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID chatId) {
        return chatService.getChat(UUID.fromString(jwt.getSubject()), chatId);
    }

    @DeleteMapping("/{chatId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteChat(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID chatId) {
        return chatService.deleteChat(UUID.fromString(jwt.getSubject()), chatId);
    }
}