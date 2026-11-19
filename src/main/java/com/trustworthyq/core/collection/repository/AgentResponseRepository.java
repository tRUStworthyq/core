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

    public Flux<JsonNode> objects(UUID requestId, UUID ownerId, long offset, int limit, String piiStatus,
                                  JsonMapper mapper) {
        return database.sql("""
                SELECT item.value::text AS object
                FROM collection_runs r JOIN agent_response_batches b
                  ON b.request_id = r.id AND b.implementation_id = r.implementation_id
                CROSS JOIN LATERAL jsonb_array_elements(b.items) WITH ORDINALITY AS item(value, position)
                WHERE r.id = :id AND r.owner_id = :owner AND b.status = 'BATCH'
                  AND (:piiStatus = '' OR coalesce(item.value->'pii'->>'status', 'NOT_SCANNED') = :piiStatus)
                ORDER BY b.batch_index, item.position
                LIMIT :limit OFFSET :offset
                """)
                .bind("id", requestId).bind("owner", ownerId)
                .bind("offset", offset).bind("limit", limit).bind("piiStatus", piiStatus)
                .map((row, metadata) -> mapper.readTree(row.get("object", String.class)))
                .all();
    }

    public Flux<ScoringObject> scoringObjects(UUID requestId, UUID ownerId, long offset, int limit,
                                             JsonMapper mapper) {
        return database.sql("""
                WITH objects AS (
                    SELECT item.value AS object, item.value->>'sha256' AS hash,
                           (item.value->>'modificationTime')::timestamptz AS modified,
                           b.batch_index, item.position, r.created_at
                    FROM collection_runs r JOIN agent_response_batches b
                      ON b.request_id = r.id AND b.implementation_id = r.implementation_id
                    CROSS JOIN LATERAL jsonb_array_elements(b.items) WITH ORDINALITY AS item(value, position)
                    WHERE r.id = :id AND r.owner_id = :owner AND b.status = 'BATCH'
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
                FROM ranked ORDER BY batch_index, position LIMIT :limit OFFSET :offset
                """)
                .bind("id", requestId).bind("owner", ownerId)
                .bind("offset", offset).bind("limit", limit)
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
}