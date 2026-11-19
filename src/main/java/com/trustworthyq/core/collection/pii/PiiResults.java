package com.trustworthyq.core.collection.pii;

import com.trustworthyq.core.collection.service.CollectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PiiResults {
    private static final String OBJECTS = """
            WITH objects AS (
                SELECT implementation_id, batch_index, position,
                       metadata->>'path' AS path,
                       CASE WHEN jsonb_typeof(metadata->'pii') = 'object' THEN metadata->'pii' END AS pii,
                       coalesce(metadata->'pii'->>'status', 'NOT_SCANNED') AS status
                FROM visible_collection_objects WHERE request_id = :run AND owner_id = :owner
            )
            """;

    private final DatabaseClient database;
    private final JsonMapper json;
    private final CollectionService collections;

    public Mono<Summary> summary(UUID owner, UUID run) {
        return collections.completed(owner, run).thenMany(database.sql(OBJECTS + """
                        SELECT status, count(*) AS files, count(*) FILTER (
                            WHERE status = 'PARTIAL'
                               OR (status IN ('DETECTED', 'REVIEW_REQUIRED') AND pii->'scan_complete' = 'false'::jsonb)
                        ) AS partial
                        FROM objects GROUP BY status
                        """).bind("owner", owner).bind("run", run)
                .map((row, metadata) -> new Count(row.get("status", String.class),
                        row.get("files", Long.class), row.get("partial", Long.class))).all())
                .collectList().map(counts -> {
                    Map<String, Long> byStatus = new LinkedHashMap<>();
                    for (Filter filter : Filter.values()) {
                        if (filter != Filter.FINDINGS && filter != Filter.ALL) {
                            byStatus.put(filter.name(), 0L);
                        }
                    }
                    counts.forEach(count -> byStatus.put(count.status(), count.files()));
                    return new Summary(counts.stream().mapToLong(Count::files).sum(),
                            counts.stream().mapToLong(Count::partial).sum(),
                            byStatus.get("NOT_SCANNED") + byStatus.get("UNSUPPORTED") + byStatus.get("ERROR"),
                            byStatus);
                });
    }

    public Mono<Page> files(UUID owner, UUID run, Filter filter, Category category, long offset, int pageSize) {
        int limit = Math.clamp(pageSize, 1, 200);
        long start = Math.max(0, offset);
        return collections.completed(owner, run).thenMany(database.sql(OBJECTS + """
                        SELECT implementation_id, path, status, pii::text AS classification FROM objects
                        WHERE (:filter = 'ALL' OR status = :filter OR
                               (:filter = 'FINDINGS' AND status IN ('DETECTED', 'REVIEW_REQUIRED')))
                          AND (:category = '' OR jsonb_exists(pii->'categories', :category))
                        ORDER BY batch_index, position LIMIT :limit OFFSET :offset
                        """).bind("owner", owner).bind("run", run).bind("filter", filter.name())
                .bind("category", category == null ? "" : category.name())
                .bind("limit", limit + 1).bind("offset", start)
                .map((row, metadata) -> {
                    String raw = row.get("classification", String.class);
                    JsonNode pii = raw == null ? json.createObjectNode() : json.readTree(raw);
                    String path = row.get("path", String.class);
                    UUID impl = row.get("implementation_id", UUID.class);
                    return new File(path == null ? null : path.substring(path.lastIndexOf('/') + 1), path,
                            row.get("status", String.class), strings(pii.path("categories")),
                            strings(pii.path("detectors")), pii.path("scan_complete").isBoolean()
                            ? pii.path("scan_complete").asBoolean() : null,
                            pii.path("scanner_version").isString() ? pii.path("scanner_version").asString() : null,
                            "NEEDS_DETERMINATION", graph(impl, path));
                }).all())
                .collectList().map(items -> new Page(items.stream().limit(limit).toList(),
                        items.size() > limit ? start + limit : null));
    }

    private static List<String> strings(JsonNode array) {
        List<String> values = new ArrayList<>();
        if (array.isArray()) {
            array.forEach(value -> {
                if (value.isString()) {
                    values.add(value.asString());
                }
            });
        }
        return values;
    }

    private static GraphLocation graph(UUID impl, String path) {
        if (path == null || !path.startsWith("s3://")) {
            return null;
        }
        int separator = path.indexOf('/', 5);
        if (separator < 6 || separator == path.length() - 1) {
            return null;
        }
        String key = path.substring(separator + 1);
        return new GraphLocation(impl, path.substring(5, separator), key.substring(0, key.lastIndexOf('/') + 1), key);
    }

    private record Count(String status, long files, long partial) {
    }

    public enum Filter {
        FINDINGS, ALL, DETECTED, REVIEW_REQUIRED, NOT_DETECTED, PARTIAL, UNSUPPORTED, ERROR, NOT_SCANNED
    }

    public enum Category {
        EMAIL, PHONE_RU, SNILS, INN_12, PASSPORT_RU
    }

    public record Summary(long total, long partiallyScanned, long notChecked, Map<String, Long> byStatus) {
    }

    public record Page(List<File> items, Long nextOffset) {
    }

    public record File(String name, String path, String status, List<String> categories, List<String> detectors,
                       Boolean scanComplete, String scannerVersion, String retentionStatus, GraphLocation graph) {
    }

    public record GraphLocation(UUID implId, String bucket, String prefix, String key) {
    }
}
