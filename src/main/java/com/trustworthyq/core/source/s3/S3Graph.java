package com.trustworthyq.core.source.s3;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class S3Graph {
    private static final String SNAPSHOT = """
            WITH snapshot AS (
                SELECT r.id
                FROM collection_runs r JOIN impl i ON i.id = r.implementation_id AND i.owner_id = r.owner_id
                JOIN agent_response_batches b ON b.request_id = r.id AND b.implementation_id = i.id
                WHERE r.owner_id = :owner AND i.id = :impl AND NOT r.dispatch_failed
                  AND i.source_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14'
                GROUP BY r.id
                HAVING count(*) FILTER (WHERE b.status = 'FAILED') = 0
                   AND max(b.batch_index) FILTER (WHERE b.status = 'COMPLETED')
                       = count(*) FILTER (WHERE b.status = 'BATCH')
                ORDER BY r.created_at DESC, r.id DESC
                LIMIT 1
            ), objects AS (
                SELECT substr(o.metadata->>'path', 6) AS location, o.metadata
                FROM snapshot s JOIN visible_collection_objects o ON o.request_id = s.id
                WHERE starts_with(o.metadata->>'path', 's3://')
            )
            """;

    private final DatabaseClient database;

    public Mono<BucketPage> buckets(UUID owner, UUID impl, int requestedLimit, String token) {
        int limit = Math.clamp(requestedLimit, 1, 200);
        return database.sql(SNAPSHOT + """
                SELECT DISTINCT split_part(location, '/', 1) AS bucket
                FROM objects WHERE split_part(location, '/', 1) > :cursor
                ORDER BY bucket LIMIT :limit
                """)
                .bind("owner", owner).bind("impl", impl).bind("limit", limit + 1)
                .bind("cursor", token == null ? "" : token)
                .map((row, ignored) -> new Bucket(row.get("bucket", String.class), ""))
                .all().collectList()
                .map(rows -> new BucketPage(rows.stream().limit(limit).toList(),
                        rows.size() > limit ? rows.get(limit - 1).name() : null));
    }

    public Mono<Children> children(UUID owner, UUID impl, String bucket, String prefix, int requestedLimit,
                                  String token) {
        String parent = prefix == null ? "" : prefix;
        if (!parent.isEmpty() && !parent.endsWith("/")) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid S3 folder prefix"));
        }
        int limit = Math.clamp(requestedLimit, 1, 200);
        return database.sql(SNAPSHOT + """
                , descendants AS (
                    SELECT substr(location, length(:root) + 1) AS suffix, metadata
                    FROM objects WHERE starts_with(location, :root)
                ), nodes AS (
                    SELECT CASE WHEN strpos(suffix, '/') = 0 THEN suffix
                                ELSE left(suffix, strpos(suffix, '/')) END AS name,
                           strpos(suffix, '/') > 0 AS folder,
                           metadata
                    FROM descendants WHERE suffix <> ''
                )
                SELECT name, folder,
                       CASE WHEN NOT folder THEN max((metadata->>'sizeBytes')::bigint) END AS size_bytes,
                       CASE WHEN NOT folder THEN max((metadata->>'modificationTime')::timestamptz) END AS modified_at
                FROM nodes WHERE name > :cursor
                GROUP BY name, folder ORDER BY name LIMIT :limit
                """)
                .bind("owner", owner).bind("impl", impl).bind("limit", limit + 1)
                .bind("root", bucket + "/" + parent).bind("cursor", token == null ? "" : token)
                .map((row, ignored) -> {
                    var modified = row.get("modified_at", OffsetDateTime.class);
                    return new Node(row.get("name", String.class), Boolean.TRUE.equals(row.get("folder", Boolean.class)),
                            row.get("size_bytes", Long.class), modified == null ? null : modified.toInstant());
                })
                .all().collectList()
                .map(rows -> {
                    var page = rows.stream().limit(limit).toList();
                    var folders = page.stream().filter(Node::folder)
                            .map(node -> new Folder(node.name(), parent + node.name())).toList();
                    var files = page.stream().filter(node -> !node.folder())
                            .map(node -> new File(node.name(), parent + node.name(), node.sizeBytes(), node.modifiedAt()))
                            .toList();
                    return new Children(parent, folders, files, rows.size() > limit ? rows.get(limit - 1).name() : null);
                });
    }

    private record Node(String name, boolean folder, Long sizeBytes, Instant modifiedAt) {
    }

    public record Bucket(String name, String rootPrefix) {
    }

    public record BucketPage(List<Bucket> buckets, String nextToken) {
    }

    public record Folder(String name, String prefix) {
    }

    public record File(String name, String key, Long sizeBytes, Instant modifiedAt) {
    }

    public record Children(String prefix, List<Folder> folders, List<File> files, String nextToken) {
    }
}
