package com.trustworthyq.core.chat.repository;

import java.time.Instant;
import java.util.UUID;

import com.trustworthyq.core.chat.entity.MessageEntity;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MessageRepository extends ReactiveCrudRepository<MessageEntity, UUID> {

    Mono<MessageEntity> findByIdAndChatIdAndDeletedAtIsNull(UUID id, UUID chatId);

    @Query("""
            SELECT * FROM messages
            WHERE chat_id = :chatId
              AND deleted_at IS NULL
              AND (created_at, id) > (:cursorCreatedAt, :cursorId)
            ORDER BY created_at ASC, id ASC
            LIMIT :limit
            """)
    Flux<MessageEntity> findPageAfterCursor(UUID chatId, Instant cursorCreatedAt, UUID cursorId, int limit);

    @Query("""
            SELECT * FROM messages
            WHERE chat_id = :chatId
              AND deleted_at IS NULL
            ORDER BY created_at ASC, id ASC
            LIMIT :limit
            """)
    Flux<MessageEntity> findFirstPage(UUID chatId, int limit);

    @Modifying
    @Query("""
            UPDATE messages
            SET deleted_at = :now,
                updated_at = :now,
                version = version + 1
            WHERE chat_id = :chatId
                AND deleted_at IS NULL
                AND (created_at, id) > (:createdAt, :id)
            """)
    Mono<Integer> softDeleteAfter(UUID chatId, Instant createdAt, UUID id, Instant now);
}