package com.trustworthyq.core.source.repository;

import com.trustworthyq.core.source.entity.Impl;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface ImplRepository extends ReactiveCrudRepository<Impl, UUID> {

    Flux<Impl> findBySourceId(UUID sourceId);

    @Query("""
    INSERT INTO "impl" ("id", "name", "config", "state", "source_id")
    VALUES (:#{#impl.id}, :#{#impl.name}, CAST(:#{#impl.config} AS jsonb), :#{#impl.state}, :#{#impl.sourceId})
    """)
    Mono<Impl> saveWithJsonbCast(@Param("impl") Impl impl);

    @Query("""
    UPDATE "impl"
    SET "name" = :#{#impl.name},
        "config" = CAST(:#{#impl.config} AS jsonb),
        "state" = :#{#impl.state},
        "source_id" = :#{#impl.sourceId}
    WHERE "id" = :#{#impl.id}
    """)
    Mono<Impl> updateWithJsonbCast(@Param("impl") Impl impl);
}