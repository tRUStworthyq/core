package com.trustworthyq.core.source.s3;

import com.trustworthyq.core.source.s3.ArchivedObject.Details;

import com.trustworthyq.core.source.s3.ArchivedObject.State;
import com.trustworthyq.core.source.s3.S3Operations.ArchiveConfirmation;
import com.trustworthyq.core.source.s3.S3Operations.ArchiveResult;
import com.trustworthyq.core.source.s3.S3Operations.ArchivePreview;
import com.trustworthyq.core.source.service.ImplService;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class S3Archive {
    private final DatabaseClient database;
    private final JsonMapper json;
    private final S3Operations storage;
    private final ImplService implementations;
    private final TransactionalOperator transaction;

    public Mono<ArchiveResult> create(UUID owner, UUID impl, ArchiveConfirmation confirmation) {
        if (!confirmation.confirmed() || confirmation.preview() == null) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit archive confirmation required"));
        }
        ArchivePreview accepted = confirmation.preview();
        return storage.archivePreview(owner, impl, accepted.source()).flatMap(preview -> {
            if (!preview.equals(accepted)) {
                return Mono.error(conflict("Archive preview changed. Confirm the new preview."));
            }
            UUID id = S3Operations.archiveId(preview.destination());
            Details provisional = new Details(preview.source(), preview.destination(), preview.sizeBytes(),
                    null, null, null, null);

            return database.sql("""
                            INSERT INTO s3_archive (id, owner_id, impl_id, state, payload)
                            VALUES (:id, :owner, :impl, 'PENDING', CAST(:payload AS jsonb))
                            ON CONFLICT (id) DO NOTHING
                            """)
                    .bind("id", id).bind("owner", owner).bind("impl", impl)
                    .bind("payload", json.writeValueAsString(provisional))
                    .fetch().rowsUpdated()
                    .then(change(owner, impl, id, State.ARCHIVED, object -> {
                        if (object.state() == State.ARCHIVED) {
                            return Mono.just(object.details());
                        }
                        if (object.state() != State.PENDING) {
                            return Mono.error(conflict("Archive entry is no longer pending"));
                        }
                        return storage.archive(owner, impl, confirmation).map(ArchiveResult::details);
                    }))
                    .map(object -> new ArchiveResult("ARCHIVED", object.details().source(),
                            object.details().destination(), object.details().archive().versionId(),
                            false, id, object.details()));
        });
    }

    public Mono<Page> page(UUID owner, UUID impl, State state, long offset, int capacity) {
        int limit = Math.clamp(capacity, 1, 200);
        long start = Math.max(0, offset);
        return implementations.findById(owner, impl).thenMany(database.sql("""
                        SELECT id, state, created_at, payload::text AS payload
                        FROM s3_archive
                        WHERE owner_id = :owner AND impl_id = :impl AND state = :state
                        ORDER BY created_at DESC, id DESC LIMIT :limit OFFSET :offset
                        """)
                        .bind("owner", owner).bind("impl", impl).bind("state", state.name())
                        .bind("limit", limit + 1).bind("offset", start)
                        .map((row, ignored) -> read(row)).all())
                .collectList()
                .map(items -> new Page(items.stream().limit(limit).toList(),
                        items.size() > limit ? start + limit : null));
    }

    public Mono<ArchivedObject> restore(UUID owner, UUID impl, UUID id) {
        return change(owner, impl, id, State.RESTORED, object -> {
            if (object.state() != State.ARCHIVED) {
                return Mono.error(conflict("Only archived objects can be restored"));
            }
            return storage.restore(owner, impl, object.details()).map(object.details()::restoredAs);
        });
    }

    public Mono<ArchivedObject> undoRestore(UUID owner, UUID impl, UUID id) {
        return change(owner, impl, id, State.ARCHIVED, object -> {
            if (object.state() != State.RESTORED) {
                return Mono.error(conflict("Only restored objects can be returned to the archive"));
            }
            return storage.verifyUndo(owner, impl, object.details()).thenReturn(object.details());
        });
    }

    public Mono<ArchivedObject> delete(UUID owner, UUID impl, UUID id) {
        return change(owner, impl, id, State.DELETED,
                object -> {
                    if (object.state() == State.PENDING) {
                        return Mono.error(conflict("Complete the pending archive operation before deleting"));
                    }
                    return storage.deleteArchive(owner, impl, object.details()).thenReturn(object.details());
                });
    }

    private Mono<ArchivedObject> change(UUID owner, UUID impl, UUID id, State state,
                                        java.util.function.Function<ArchivedObject, Mono<Details>> action) {
        return database.sql("""
                        SELECT id, state, created_at, payload::text AS payload
                        FROM s3_archive WHERE id = :id AND owner_id = :owner AND impl_id = :impl FOR UPDATE
                        """).bind("id", id).bind("owner", owner).bind("impl", impl)
                .map((row, ignored) -> read(row)).one()
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Archive entry not found")))
                .flatMap(object -> {
                    if (object.state() == state && state != State.ARCHIVED) {
                        return Mono.just(object);
                    }
                    if (object.state() == State.DELETED) {
                        return Mono.error(conflict("Archive copy has been deleted"));
                    }
                    return action.apply(object).flatMap(details -> database.sql("""
                                    UPDATE s3_archive SET state = :state, payload = CAST(:payload AS jsonb),
                                        deleted_at = CASE WHEN :state = 'DELETED' THEN statement_timestamp() END
                                    WHERE id = :id AND owner_id = :owner AND impl_id = :impl
                                    """)
                            .bind("id", id).bind("owner", owner).bind("impl", impl)
                            .bind("state", state.name()).bind("payload", json.writeValueAsString(details))
                            .fetch().rowsUpdated()
                            .thenReturn(new ArchivedObject(id, state, object.createdAt(), details)));
                }).as(transaction::transactional);
    }

    private ArchivedObject read(Row row) {
        return new ArchivedObject(row.get("id", UUID.class), State.valueOf(row.get("state", String.class)),
                row.get("created_at", OffsetDateTime.class).toInstant(),
                json.readValue(row.get("payload", String.class), Details.class));
    }

    private static ResponseStatusException conflict(String message) {
        return new ResponseStatusException(HttpStatus.CONFLICT, message);
    }

    public record Page(List<ArchivedObject> items, Long nextOffset) {
    }
}
