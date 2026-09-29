package com.trustworthyq.core.chat.repository;

import java.time.Instant;
import java.util.UUID;

import com.trustworthyq.core.chat.entity.ChatEntity;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ChatRepository extends ReactiveCrudRepository<ChatEntity, UUID> {

    Mono<ChatEntity> findByIdAndOwnerId(UUID id, UUID ownerId);

    @Query("""
            SELECT * FROM chats
            WHERE owner_id = :ownerId
              AND (created_at, id) < (:cursorCreatedAt, :cursorId)
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """)
    Flux<ChatEntity> findPageAfterCursor(UUID ownerId, Instant cursorCreatedAt, UUID cursorId, int limit);

    @Query("""
            SELECT * FROM chats
            WHERE owner_id = :ownerId
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """)
    Flux<ChatEntity> findFirstPage(UUID ownerId, int limit);
}