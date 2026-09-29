package com.trustworthyq.core.chat.service.impl;

import com.trustworthyq.core.chat.config.PaginationProperties;
import com.trustworthyq.core.chat.dto.ChatResponse;
import com.trustworthyq.core.chat.dto.CursorPage;
import com.trustworthyq.core.chat.entity.ChatEntity;
import com.trustworthyq.core.chat.exception.ChatNotFoundException;
import com.trustworthyq.core.chat.repository.ChatRepository;
import com.trustworthyq.core.chat.service.ChatService;
import com.trustworthyq.core.common.CursorCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class DefaultChatService implements ChatService {

    private final ChatRepository chatRepository;
    private final PaginationProperties paginationProperties;

    public Mono<ChatResponse> createChat(UUID ownerId, String title) {
        return chatRepository.save(ChatEntity.create(ownerId, title))
                .map(ChatResponse::from);
    }

    public Mono<ChatResponse> getChat(UUID ownerId, UUID chatId) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .map(ChatResponse::from)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)));
    }

    // Физическое удаление: сообщения под этим chatId подчищаются каскадом
    // на уровне БД (FK messages.chat_id -> chats.id ON DELETE CASCADE).
    public Mono<Void> deleteChat(UUID ownerId, UUID chatId) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .flatMap(chatRepository::delete);
    }

    public Mono<CursorPage<ChatResponse>> listChats(UUID ownerId, String cursor, Integer limit) {
        int pageSize = (limit == null || limit <= 0) ? paginationProperties.chatPageSize() : limit;

        Flux<ChatEntity> source = (cursor == null)
                ? chatRepository.findFirstPage(ownerId, pageSize + 1)
                : decodeAndQuery(ownerId, cursor, pageSize + 1);

        return source.collectList().map(chats -> toPage(chats, pageSize));
    }

    private Flux<ChatEntity> decodeAndQuery(UUID ownerId, String cursor, int limitPlusOne) {
        CursorCodec.Cursor decoded = CursorCodec.decode(cursor);
        return chatRepository.findPageAfterCursor(ownerId, decoded.createdAt(), decoded.id(), limitPlusOne);
    }

    private CursorPage<ChatResponse> toPage(List<ChatEntity> chats, int pageSize) {
        boolean hasMore = chats.size() > pageSize;
        List<ChatEntity> pageItems = hasMore ? chats.subList(0, pageSize) : chats;

        String nextCursor = null;
        if (hasMore) {
            ChatEntity last = pageItems.get(pageItems.size() - 1);
            nextCursor = CursorCodec.encode(last.getCreatedAt(), last.getId());
        }

        List<ChatResponse> responses = new ArrayList<>(pageItems.size());
        pageItems.forEach(chat -> responses.add(ChatResponse.from(chat)));

        return new CursorPage<>(responses, nextCursor, hasMore);
    }
}