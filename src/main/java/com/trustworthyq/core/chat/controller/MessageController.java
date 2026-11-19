package com.trustworthyq.core.chat.controller;

import com.trustworthyq.core.chat.dto.CreateMessageRequest;
import com.trustworthyq.core.chat.dto.CursorPage;
import com.trustworthyq.core.chat.dto.MessageResponse;
import com.trustworthyq.core.chat.dto.UpdateMessageRequest;
import com.trustworthyq.core.chat.service.MessageService;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/chats/{chatId}/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageService messageService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<MessageResponse> sendMessage(@AuthenticationPrincipal Jwt jwt,
                                             @PathVariable UUID chatId,
                                             @Valid @RequestBody CreateMessageRequest request) {
        return messageService.sendMessage(UUID.fromString(jwt.getSubject()), chatId, request.content(), request.requestId());
    }

    @GetMapping
    public Mono<CursorPage<MessageResponse>> listMessages(@AuthenticationPrincipal Jwt jwt,
                                                          @PathVariable UUID chatId,
                                                          @RequestParam(required = false) String cursor,
                                                          @RequestParam(required = false) Integer limit) {
        return messageService.listMessages(UUID.fromString(jwt.getSubject()), chatId, cursor, limit);
    }

    @PatchMapping("/{messageId}")
    public Mono<MessageResponse> editMessage(@AuthenticationPrincipal Jwt jwt,
                                             @PathVariable UUID chatId,
                                             @PathVariable UUID messageId,
                                             @Valid @RequestBody UpdateMessageRequest request) {
        return messageService.editMessage(UUID.fromString(jwt.getSubject()), chatId, messageId, request.content());
    }

    @DeleteMapping("/{messageId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteMessage(@AuthenticationPrincipal Jwt jwt,
                                    @PathVariable UUID chatId,
                                    @PathVariable UUID messageId) {
        return messageService.deleteMessage(UUID.fromString(jwt.getSubject()), chatId, messageId);
    }
}