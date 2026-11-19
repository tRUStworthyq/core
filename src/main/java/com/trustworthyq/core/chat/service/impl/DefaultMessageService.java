package com.trustworthyq.core.chat.service.impl;

import com.trustworthyq.core.ai.service.impl.OllamaChatServiceImpl;
import com.trustworthyq.core.chat.config.PaginationProperties;
import com.trustworthyq.core.chat.dto.ChatStreamEvent;
import com.trustworthyq.core.chat.dto.CursorPage;
import com.trustworthyq.core.chat.dto.MessageResponse;
import com.trustworthyq.core.chat.entity.MessageEntity;
import com.trustworthyq.core.chat.entity.ChatEntity;
import com.trustworthyq.core.chat.exception.ChatNotFoundException;
import com.trustworthyq.core.chat.exception.MessageNotFoundException;
import com.trustworthyq.core.chat.repository.ChatRepository;
import com.trustworthyq.core.chat.repository.MessageRepository;
import com.trustworthyq.core.chat.service.MessageService;
import com.trustworthyq.core.common.CursorCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class DefaultMessageService implements MessageService {

    private static final int AI_HISTORY_LIMIT = 30;

    private final ChatRepository chatRepository;
    private final MessageRepository messageRepository;
    private final PaginationProperties paginationProperties;
    private final OllamaChatServiceImpl ollama;
    private final TransactionalOperator transaction;

    public Mono<MessageResponse> sendMessage(UUID ownerId, UUID chatId, String content, UUID requestId) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .then(recentHistory(chatId))
                .flatMap(history -> ollama.chat(history, content, ownerId, requestId)
                        .flatMap(reply -> saveTurn(ownerId, chatId, history, content, reply)));
    }

    @Override
    public Flux<ChatStreamEvent> streamMessage(UUID ownerId, UUID chatId, String content, UUID requestId) {
        return chatRepository.findByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .then(recentHistory(chatId))
                .flatMapMany(history -> streamTurn(history, ownerId, content, requestId,
                        reply -> saveTurn(ownerId, chatId, history, content, reply).thenReturn(chatId)));
    }

    @Override
    public Flux<ChatStreamEvent> streamNewChat(UUID ownerId, String content, UUID requestId) {
        return streamTurn(List.of(), ownerId, content, requestId,
                reply -> chatRepository.save(ChatEntity.create(ownerId, DefaultChatService.DEFAULT_TITLE))
                        .flatMap(chat -> saveMessages(chat.getId(), content, reply).thenReturn(chat.getId()))
                        .as(transaction::transactional));
    }

    private Flux<ChatStreamEvent> streamTurn(List<MessageResponse> history, UUID ownerId, String content,
                                             UUID requestId, Function<String, Mono<UUID>> save) {
        return Flux.defer(() -> {
            var reply = new StringBuilder();
            var deltas = ollama.stream(history, content, ownerId, requestId)
                    .map(chunk -> {
                        reply.append(chunk);
                        return ChatStreamEvent.delta(chunk);
                    });
            var done = Mono.defer(() -> reply.toString().isBlank()
                    ? Mono.<UUID>error(new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Ollama returned an empty reply"))
                    : save.apply(reply.toString())).map(ChatStreamEvent::done);
            return Flux.concat(Mono.just(ChatStreamEvent.phase(requestId == null ? "GENERATING" : "ANALYZING")),
                            deltas, done)
                    .onErrorResume(error -> {
                        if (error instanceof ResponseStatusException failure && failure.getReason() != null) {
                            return Mono.just(ChatStreamEvent.error(failure.getReason()));
                        }
                        log.warn("Chat stream failed: {}", error.getClass().getSimpleName());
                        return Mono.just(ChatStreamEvent.error("Chat response could not be saved or generated"));
                    });
        });
    }

    private Mono<List<MessageResponse>> recentHistory(UUID chatId) {
        return messageRepository.findRecentMessages(chatId, AI_HISTORY_LIMIT)
                .map(MessageResponse::from)
                .collectList()
                .map(messages -> List.copyOf(messages.reversed()));
    }

    private Mono<MessageResponse> saveTurn(UUID ownerId, UUID chatId, List<MessageResponse> history,
                                           String content, String reply) {
        return lockChat(ownerId, chatId)
                .then(recentHistory(chatId))
                .filter(history::equals)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.CONFLICT, "Chat changed during generation")))
                .flatMap(current -> saveMessages(chatId, content, reply))
                .as(transaction::transactional);
    }

    private Mono<MessageResponse> saveMessages(UUID chatId, String content, String reply) {
        return messageRepository.save(MessageEntity.userMessage(chatId, content))
                .flatMap(user -> messageRepository.save(MessageEntity.agentMessage(chatId, reply))
                        .flatMap(agent -> chatRepository.touch(chatId, agent.getCreatedAt()))
                        .thenReturn(MessageResponse.from(user)));
    }

    private Mono<Void> lockChat(UUID ownerId, UUID chatId) {
        return chatRepository.lockByIdAndOwnerId(chatId, ownerId)
                .switchIfEmpty(Mono.error(new ChatNotFoundException(chatId)))
                .then();
    }

    @Transactional
    public Mono<MessageResponse> editMessage(UUID ownerId, UUID chatId, UUID messageId, String newContent) {
        return lockChat(ownerId, chatId)
                .then(messageRepository.findByIdAndChatIdAndDeletedAtIsNull(messageId, chatId))
                .switchIfEmpty(Mono.error(new MessageNotFoundException(messageId)))
                .flatMap(message -> {
                    message.edit(newContent);
                    return messageRepository.save(message);
                })
                .flatMap(edited -> messageRepository
                        .softDeleteAfter(
                                chatId,
                                edited.getCreatedAt(),
                                edited.getId(),
                                edited.getUpdatedAt())
                        .thenReturn(edited))
                .map(MessageResponse::from);
    }

    @Transactional
    public Mono<Void> deleteMessage(UUID ownerId, UUID chatId, UUID messageId) {
        return lockChat(ownerId, chatId)
                .then(messageRepository.findByIdAndChatIdAndDeletedAtIsNull(messageId, chatId))
                .switchIfEmpty(Mono.error(new MessageNotFoundException(messageId)))
                .flatMap(message -> {
                    message.delete();
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
