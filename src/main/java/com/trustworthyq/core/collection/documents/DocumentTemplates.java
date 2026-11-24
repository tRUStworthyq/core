package com.trustworthyq.core.collection.documents;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DocumentTemplates {
    private final DatabaseClient database;

    public Flux<Template> list(UUID owner) {
        return database.sql("""
                SELECT id, name, path_contains, name_contains, content_contains, retention_days, retention_years
                FROM document_templates WHERE owner_id = :owner ORDER BY name, id
                """).bind("owner", owner).map((row, ignored) -> new Template(row.get("id", UUID.class),
                row.get("name", String.class), row.get("path_contains", String.class),
                row.get("name_contains", String.class), row.get("content_contains", String.class),
                row.get("retention_days", Integer.class), row.get("retention_years", Integer.class))).all();
    }

    public Mono<Map<UUID, String>> contentRules(UUID owner) {
        return list(owner).filter(rule -> !rule.contentContains().isEmpty()).collectList()
                .map(rules -> rules.stream().collect(Collectors.toUnmodifiableMap(Template::id, Template::contentContains)));
    }

    public Mono<Template> save(UUID owner, UUID previous, Template value) {
        String name = clean(value.name());
        String path = clean(value.pathContains());
        String filename = clean(value.nameContains());
        String content = clean(value.contentContains());
        Integer days = value.retentionDays();
        Integer years = value.retentionYears();
        boolean validDuration = (days != null && days >= 1 && days <= 36500 && years == null)
                || (years != null && years >= 1 && years <= 100 && days == null);
        if (name.isEmpty() || name.length() > 120 || path.length() > 512 || filename.length() > 512
                || content.length() > 512 || (path + filename + content).isEmpty() || !validDuration) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Укажите название, условие и срок хранения в днях (1–36500) или годах (1–100).");
        }
        // A changed content condition must receive a new ID so an old scan cannot satisfy it.
        UUID id = UUID.randomUUID();
        var query = database.sql(previous == null ? """
                INSERT INTO document_templates (owner_id, id, name, path_contains, name_contains, content_contains, retention_days, retention_years)
                VALUES (:owner, :id, :name, :path, :filename, :content, :days, :years)
                """ : """
                UPDATE document_templates SET id = CASE WHEN content_contains = :content THEN id ELSE :id END,
                    name = :name, path_contains = :path, name_contains = :filename,
                    content_contains = :content, retention_days = :days, retention_years = :years
                WHERE owner_id = :owner AND id = :previous
                RETURNING id
                """).bind("owner", owner).bind("id", id).bind("name", name).bind("path", path)
                .bind("filename", filename).bind("content", content);
        query = days == null ? query.bindNull("days", Integer.class) : query.bind("days", days);
        query = years == null ? query.bindNull("years", Integer.class) : query.bind("years", years);
        Mono<UUID> saved = previous == null ? query.fetch().rowsUpdated().thenReturn(id)
                : query.bind("previous", previous).map((row, ignored) -> row.get("id", UUID.class)).one()
                    .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Шаблон не найден.")));
        return saved.map(key -> new Template(key, name, path, filename, content, days, years));
    }

    public Mono<Void> delete(UUID owner, UUID id) {
        return database.sql("DELETE FROM document_templates WHERE owner_id = :owner AND id = :id")
                .bind("owner", owner).bind("id", id).fetch().rowsUpdated().then();
    }

    private static String clean(String value) {
        return value == null ? "" : value.strip();
    }

    public record Template(UUID id, String name, String pathContains, String nameContains,
                           String contentContains, Integer retentionDays, Integer retentionYears) {
        public Template(UUID id, String name, String pathContains, String nameContains, String contentContains,
                        int retentionDays) {
            this(id, name, pathContains, nameContains, contentContains, retentionDays, null);
        }

        Instant retainUntil(Instant modified) {
            if (modified == null) return null;
            return retentionYears == null ? modified.plus(retentionDays, ChronoUnit.DAYS)
                    : modified.atOffset(ZoneOffset.UTC).plusYears(retentionYears).toInstant();
        }
    }
}
