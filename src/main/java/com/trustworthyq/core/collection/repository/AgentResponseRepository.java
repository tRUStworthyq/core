package com.trustworthyq.core.collection.repository;

import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class AgentResponseRepository {
    private final DatabaseClient database;

    public AgentResponseRepository(DatabaseClient database) {
        this.database = database;
    }

    public Mono<Void> save(String payload) {
        return database.sql("""
                INSERT INTO agent_response_batches
                    (request_id, implementation_id, source_id, batch_index, status, items, error)
                SELECT "requestId", id, "sourceId", "batchIndex", status, items, error
                FROM jsonb_to_record(CAST(:payload AS jsonb)) AS response(
                    "requestId" uuid, id uuid, "sourceId" uuid, "batchIndex" bigint,
                    status text, items jsonb, error text
                )
                ON CONFLICT (request_id, batch_index) DO NOTHING
                """)
                .bind("payload", payload)
                .fetch()
                .rowsUpdated()
                .then();
    }

    public Mono<Void> start(UUID requestId, UUID ownerId, UUID implementationId) {
        return database.sql("""
                INSERT INTO collection_runs (id, owner_id, implementation_id)
                VALUES (:id, :owner, :implementation)
                """)
                .bind("id", requestId).bind("owner", ownerId).bind("implementation", implementationId)
                .fetch().rowsUpdated().then();
    }

    public Mono<Void> dispatchFailed(UUID requestId) {
        return database.sql("UPDATE collection_runs SET dispatch_failed = true WHERE id = :id")
                .bind("id", requestId).fetch().rowsUpdated().then();
    }

    public Mono<RunStatus> status(UUID requestId, UUID ownerId) {
        return database.sql("""
                SELECT CASE
                        WHEN count(*) FILTER (WHERE b.status = 'FAILED') > 0 OR r.dispatch_failed THEN 'FAILED'
                        WHEN max(b.batch_index) FILTER (WHERE b.status = 'COMPLETED')
                             = count(*) FILTER (WHERE b.status = 'BATCH') THEN 'COMPLETED'
                        WHEN count(b.batch_index) > 0 THEN 'RUNNING'
                        ELSE 'QUEUED' END AS status,
                       coalesce(sum(jsonb_array_length(b.items)), 0) AS processed,
                       max(b.error) AS error
                FROM collection_runs r LEFT JOIN agent_response_batches b
                  ON b.request_id = r.id AND b.implementation_id = r.implementation_id
                WHERE r.id = :id AND r.owner_id = :owner
                GROUP BY r.id
                """)
                .bind("id", requestId).bind("owner", ownerId)
                .map((row, metadata) -> new RunStatus(requestId, row.get("status", String.class),
                        row.get("processed", Long.class), row.get("error", String.class)))
                .one();
    }

    public Flux<RunHistoryItem> history(UUID ownerId, UUID implId, long offset, int limit) {
        return database.sql("""
                WITH page AS (
                    SELECT id, created_at, dispatch_failed FROM collection_runs
                    WHERE owner_id = :owner AND implementation_id = :impl
                    ORDER BY created_at DESC, id DESC LIMIT :limit OFFSET :offset
                )
                SELECT p.id, p.created_at,
                       CASE WHEN p.dispatch_failed OR count(*) FILTER (WHERE b.status = 'FAILED') > 0 THEN 'FAILED'
                            WHEN max(b.batch_index) FILTER (WHERE b.status = 'COMPLETED')
                                 = count(*) FILTER (WHERE b.status = 'BATCH') THEN 'COMPLETED'
                            WHEN count(b.batch_index) > 0 THEN 'RUNNING'
                            ELSE 'QUEUED' END AS status,
                       coalesce(sum(jsonb_array_length(b.items)), 0) AS processed,
                       max(b.error) AS error
                FROM page p LEFT JOIN agent_response_batches b
                  ON b.request_id = p.id AND b.implementation_id = :impl
                GROUP BY p.id, p.created_at, p.dispatch_failed
                ORDER BY p.created_at DESC, p.id DESC
                """)
                .bind("owner", ownerId).bind("impl", implId).bind("offset", offset).bind("limit", limit)
                .map((row, ignored) -> new RunHistoryItem(row.get("id", UUID.class),
                        row.get("created_at", java.time.OffsetDateTime.class).toInstant(),
                        row.get("status", String.class), row.get("processed", Long.class),
                        row.get("error", String.class)))
                .all();
    }

    public Flux<JsonNode> objects(UUID requestId, UUID ownerId, long offset, int limit, String piiStatus,
                                  JsonMapper mapper) {
        return database.sql("""
                SELECT metadata::text AS object
                FROM visible_collection_objects
                WHERE request_id = :id AND owner_id = :owner
                  AND (:piiStatus = '' OR coalesce(metadata->'pii'->>'status', 'NOT_SCANNED') = :piiStatus)
                ORDER BY batch_index, position
                LIMIT :limit OFFSET :offset
                """)
                .bind("id", requestId).bind("owner", ownerId)
                .bind("offset", offset).bind("limit", limit).bind("piiStatus", piiStatus)
                .map((row, metadata) -> mapper.readTree(row.get("object", String.class)))
                .all();
    }

    public Flux<ScoringObject> scoringObjects(UUID requestId, UUID ownerId, long offset, int limit,
                                             JsonMapper mapper) {
        return scoringObjects(requestId, ownerId, offset, limit, mapper, false);
    }

    public Flux<ScoringObject> recommendationCandidates(UUID requestId, UUID ownerId, JsonMapper mapper) {
        return scoringObjects(requestId, ownerId, 0, null, mapper, true);
    }

    private Flux<ScoringObject> scoringObjects(UUID requestId, UUID ownerId, long offset, Integer limit,
                                               JsonMapper mapper, boolean recommendationsOnly) {
        var query = database.sql("""
                WITH objects AS (
                    SELECT metadata AS object, metadata->>'sha256' AS hash,
                           (metadata->>'modificationTime')::timestamptz AS modified,
                           batch_index, position, created_at
                    FROM visible_collection_objects WHERE request_id = :id AND owner_id = :owner
                ), ranked AS (
                    SELECT *, max(modified) OVER duplicates AS newest,
                           count(*) OVER duplicates AS copies,
                           first_value(object->>'path') OVER (
                               PARTITION BY hash ORDER BY modified DESC NULLS LAST, object->>'path',
                                                          batch_index, position
                           ) AS preferred
                    FROM objects
                    WINDOW duplicates AS (PARTITION BY hash)
                )
                SELECT object::text, created_at, hash IS NOT NULL AND modified < newest AS newer_duplicate,
                       CASE WHEN hash IS NOT NULL AND copies > 1 THEN preferred END AS preferred
                FROM ranked
                WHERE NOT :recommendationsOnly OR coalesce(object->'pii'->>'status', '')
                    NOT IN ('DETECTED', 'REVIEW_REQUIRED', 'PARTIAL', 'ERROR')
                ORDER BY batch_index, position LIMIT :limit OFFSET :offset
                """)
                .bind("id", requestId).bind("owner", ownerId)
                .bind("offset", offset).bind("recommendationsOnly", recommendationsOnly);
        var bounded = limit == null ? query.bindNull("limit", Integer.class)
                .filter(statement -> statement.fetchSize(256)) : query.bind("limit", limit);
        return bounded
                .map((row, metadata) -> new ScoringObject(mapper.readTree(row.get("object", String.class)),
                        row.get("created_at", java.time.OffsetDateTime.class).toInstant(),
                        Boolean.TRUE.equals(row.get("newer_duplicate", Boolean.class)),
                        row.get("preferred", String.class)))
                .all();
    }

    public record ScoringObject(JsonNode object, java.time.Instant asOf, boolean newerDuplicate,
                                String preferredPath) {
    }

    public record RunStatus(UUID requestId, String status, long processed, String error) {
    }

    public record RunHistoryItem(UUID requestId, java.time.Instant createdAt, String status,
                                 long processed, String error) {
    }
}
