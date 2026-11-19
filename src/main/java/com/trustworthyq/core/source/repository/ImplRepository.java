package com.trustworthyq.core.source.repository;

import com.trustworthyq.core.source.entity.Impl;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.domain.Pageable;
import java.util.UUID;

public interface ImplRepository extends ReactiveCrudRepository<Impl, UUID> {

    Flux<Impl> findByOwnerId(UUID ownerId, Pageable pageable);
    Mono<Long> countByOwnerId(UUID ownerId);
    Mono<Impl> findByIdAndOwnerId(UUID id, UUID ownerId);
    Flux<Impl> findBySourceIdAndOwnerId(UUID sourceId, UUID ownerId, Pageable pageable);
    Mono<Long> countBySourceIdAndOwnerId(UUID sourceId, UUID ownerId);

    @Query("""
    INSERT INTO "impl" ("id", "name", "config", "state", "source_id", "owner_id")
    VALUES (:#{#impl.id}, :#{#impl.name}, CAST(:#{#impl.config} AS jsonb), :#{#impl.state}, :#{#impl.sourceId}, :#{#impl.ownerId})
    RETURNING *
    """)
    Mono<Impl> saveWithJsonbCast(@Param("impl") Impl impl);

    @Query("""
    UPDATE "impl"
    SET "name" = :#{#impl.name},
        "config" = CAST(:#{#impl.config} AS jsonb),
        "state" = :#{#impl.state},
        "source_id" = :#{#impl.sourceId}
    WHERE "id" = :#{#impl.id} AND "owner_id" = :#{#impl.ownerId}
    RETURNING *
    """)
    Mono<Impl> updateWithJsonbCast(@Param("impl") Impl impl);
}