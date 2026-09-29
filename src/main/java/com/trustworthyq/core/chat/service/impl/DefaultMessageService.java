package com.trustworthyq.core.chat.service.impl;

import com.trustworthyq.core.chat.config.PaginationProperties;
import com.trustworthyq.core.chat.dto.CursorPage;
import com.trustworthyq.core.chat.dto.MessageResponse;
import com.trustworthyq.core.chat.entity.ChatEntity;
import com.trustworthyq.core.chat.entity.MessageEntity;
import com.trustworthyq.core.chat.exception.ChatNotFoundException;
import com.trustworthyq.core.chat.exception.MessageNotFoundException;
import com.trustworthyq.core.chat.repository.ChatRepository;
import com.trustworthyq.core.chat.repository.MessageRepository;
import com.trustworthyq.core.chat.service.MessageService;
import com.trustworthyq.core.common.CursorCodec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class DefaultMessageService implements MessageService {

    private final ChatRepository chatRepository;
    private final MessageRepository messageRepository;
    private final PaginationProperties paginationProperties;

    @Transactional
    public Mono<MessageResponse> sendMessage(UUID ownerId, UUID chatId, String content) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .flatMap(chat -> messageRepository.save(MessageEntity.create(chatId, content))
                        .flatMap(saved -> touchChat(chat).thenReturn(saved)))
                .map(MessageResponse::from);
    }

    public Mono<MessageResponse> editMessage(UUID ownerId, UUID chatId, UUID messageId, String newContent) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .flatMap(chat -> messageRepository.findByIdAndChatIdAndDeletedAtIsNull(messageId, chatId))
                .switchIfEmpty(Mono.error(new MessageNotFoundException(messageId)))
                .flatMap(message -> {
                    message.setContent(newContent);
                    message.setEdited(true);
                    message.setUpdatedAt(Instant.now());
                    return messageRepository.save(message);
                })
                .map(MessageResponse::from);
    }

    public Mono<Void> deleteMessage(UUID ownerId, UUID chatId, UUID messageId) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .flatMap(chat -> messageRepository.findByIdAndChatIdAndDeletedAtIsNull(messageId, chatId))
                .switchIfEmpty(Mono.error(new MessageNotFoundException(messageId)))
                .flatMap(message -> {
                    message.setDeletedAt(Instant.now());
                    return messageRepository.save(message);
                })
                .then();
    }

    public Mono<CursorPage<MessageResponse>> listMessages(UUID ownerId, UUID chatId, String cursor, Integer limit) {
        int pageSize = (limit == null || limit <= 0) ? paginationProperties.messagePageSize() : limit;

        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .flatMap(chat -> {
                    Flux<MessageEntity> source = (cursor == null)
                            ? messageRepository.findFirstPage(chatId, pageSize + 1)
                            : decodeAndQuery(chatId, cursor, pageSize + 1);
                    return source.collectList().map(messages -> toPage(messages, pageSize));
                });
    }

    private Mono<ChatEntity> touchChat(ChatEntity chat) {
        chat.setUpdatedAt(Instant.now());
        return chatRepository.save(chat);
    }

    private Flux<MessageEntity> decodeAndQuery(UUID chatId, String cursor, int limitPlusOne) {
        CursorCodec.Cursor decoded = CursorCodec.decode(cursor);
        return messageRepository.findPageAfterCursor(chatId, decoded.createdAt(), decoded.id(), limitPlusOne);
    }

    private CursorPage<MessageResponse> toPage(List<MessageEntity> messages, int pageSize) {
        boolean hasMore = messages.size() > pageSize;
        List<MessageEntity> pageItems = hasMore ? messages.subList(0, pageSize) : messages;

        String nextCursor = null;
        if (hasMore) {
            MessageEntity last = pageItems.get(pageItems.size() - 1);
            nextCursor = CursorCodec.encode(last.getCreatedAt(), last.getId());
        }

        List<MessageResponse> responses = new ArrayList<>(pageItems.size());
        pageItems.forEach(message -> responses.add(MessageResponse.from(message)));

        return new CursorPage<>(responses, nextCursor, hasMore);
    }
}