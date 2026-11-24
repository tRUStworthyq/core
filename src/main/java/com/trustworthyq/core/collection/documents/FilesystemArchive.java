package com.trustworthyq.core.collection.documents;

import com.trustworthyq.core.collection.documents.DocumentIndex.FileVersion;
import com.trustworthyq.core.source.service.ImplService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Service
public class FilesystemArchive {
    private static final UUID FILESYSTEM = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15");
    private static final String FIELDS = "id, path, size_bytes, created_at";
    private final DocumentIndex documents;
    private final DatabaseClient database;
    private final ImplService implementations;
    private final TransactionalOperator transaction;
    private final JsonMapper json;
    private final String sourceRoot;
    private final String archiveRoot;

    public FilesystemArchive(DocumentIndex documents, DatabaseClient database, ImplService implementations,
                      TransactionalOperator transaction, JsonMapper json,
                      @Value("${app.filesystem.source-root:}") String sourceRoot,
                      @Value("${app.filesystem.archive-root:}") String archiveRoot) {
        this.documents = documents;
        this.database = database;
        this.implementations = implementations;
        this.transaction = transaction;
        this.json = json;
        this.sourceRoot = sourceRoot;
        this.archiveRoot = archiveRoot;
    }

    public Mono<Page> page(UUID owner, UUID impl, long offset, int requestedLimit) {
        int limit = Math.clamp(requestedLimit, 1, 100);
        long start = Math.max(0, offset);
        return config(owner, impl).thenMany(database.sql("SELECT " + FIELDS + """
                 FROM filesystem_archive WHERE owner_id = :owner AND impl_id = :impl
                 ORDER BY created_at DESC, id DESC LIMIT :limit OFFSET :offset
                """).bind("owner", owner).bind("impl", impl).bind("limit", limit + 1).bind("offset", start)
                .map(FilesystemArchive::read).all()).collectList()
                .map(entries -> new Page(entries.stream().limit(limit).toList(), entries.size() > limit ? start + limit : null));
    }

    public Mono<Entry> create(UUID owner, UUID impl, String path) {
        if (path == null || !path.startsWith("file:///")) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Укажите путь файла из завершённого сбора."));
        }
        return config(owner, impl).flatMap(directory -> documents.load(owner, impl, null).flatMap(index -> {
            FileVersion version = index.fileVersionForArchive(path);
            UUID id = UUID.nameUUIDFromBytes((owner + "/" + impl + "/" + version.sha256() + "/" + path)
                    .getBytes(StandardCharsets.UTF_8));
            return database.sql("""
                    INSERT INTO filesystem_archive (id, owner_id, impl_id, path, size_bytes)
                    VALUES (:id, :owner, :impl, :path, 0) ON CONFLICT (id) DO NOTHING
                    """).bind("id", id).bind("owner", owner).bind("impl", impl).bind("path", path)
                    .fetch().rowsUpdated().then(find(owner, impl, id, true))
                    .flatMap(entry -> Mono.fromCallable(() -> copy(id, directory, path, version))
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(size -> database.sql("""
                                    UPDATE filesystem_archive SET size_bytes = :size WHERE id = :id
                                    RETURNING
                                    """ + FIELDS).bind("size", size).bind("id", id).map(FilesystemArchive::read).one()))
                    .as(transaction::transactional);
        }));
    }

    public Mono<Download> download(UUID owner, UUID impl, UUID id) {
        return config(owner, impl).then(find(owner, impl, id, false)).flatMap(entry ->
                Mono.fromCallable(() -> {
                    Path stored = target(id);
                    if (!Files.isRegularFile(stored, LinkOption.NOFOLLOW_LINKS)) throw missing();
                    return new Download(entry, new FileSystemResource(stored));
                }).subscribeOn(Schedulers.boundedElastic()));
    }

    private Mono<String> config(UUID owner, UUID impl) {
        return implementations.findById(owner, impl).map(connection -> {
            if (!FILESYSTEM.equals(connection.getSourceId())) throw missing();
            String directory = json.readTree(connection.getConfig()).path("directory").asString("");
            if (directory.isBlank()) throw DocumentIndex.conflict("Подключение файловой системы не настроено.");
            return directory;
        });
    }

    private Mono<Entry> find(UUID owner, UUID impl, UUID id, boolean lock) {
        return database.sql("SELECT " + FIELDS + """
                 FROM filesystem_archive WHERE owner_id = :owner AND impl_id = :impl AND id = :id
                """ + (lock ? " FOR UPDATE" : "")).bind("owner", owner).bind("impl", impl).bind("id", id)
                .map(FilesystemArchive::read).one().switchIfEmpty(Mono.error(missing()));
    }

    private long copy(UUID id, String directory, String path, FileVersion version) throws Exception {
        if (sourceRoot.isBlank()) throw unavailable();
        Path root = Path.of(sourceRoot).toRealPath();
        Path connection = root.resolve(relative(directory)).normalize();
        Path source = root.resolve(relative(path.substring("file:///".length()))).normalize();
        if (!connection.startsWith(root) || !source.startsWith(connection)
                || !source.equals(source.toRealPath()) || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)
                || !Files.getLastModifiedTime(source, LinkOption.NOFOLLOW_LINKS).toInstant().equals(version.modified())) {
            throw changed();
        }
        Path destination = target(id);
        Path temporary = Files.createTempFile(destination.getParent(), "archive-", ".tmp");
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            long size;
            try (var input = new DigestInputStream(Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS), digest);
                 var output = Files.newOutputStream(temporary)) {
                size = input.transferTo(output);
            }
            if (!HexFormat.of().formatHex(digest.digest()).equals(version.sha256())
                    || !Files.getLastModifiedTime(source, LinkOption.NOFOLLOW_LINKS).toInstant().equals(version.modified())) {
                throw changed();
            }
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return size;
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private Path target(UUID id) throws IOException {
        if (archiveRoot.isBlank()) throw unavailable();
        return Files.createDirectories(Path.of(archiveRoot)).toRealPath().resolve(id + ".bin");
    }

    private static Path relative(String value) {
        Path path = Path.of(value);
        if (path.getRoot() != null || value.matches("(?s)^[A-Za-z]:.*") || value.startsWith("\\")) throw changed();
        return path;
    }

    private static Entry read(io.r2dbc.spi.Row row, io.r2dbc.spi.RowMetadata ignored) {
        return new Entry(row.get("id", UUID.class), row.get("path", String.class), row.get("size_bytes", Long.class),
                row.get("created_at", OffsetDateTime.class).toInstant());
    }

    private static ResponseStatusException changed() {
        return DocumentIndex.conflict("Исходный файл изменился или недоступен. Выполните сбор заново.");
    }

    private static ResponseStatusException missing() {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Архивная копия не найдена.");
    }

    private static ResponseStatusException unavailable() {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Хранилище архивных копий не настроено.");
    }

    public record Entry(UUID id, String path, long sizeBytes, Instant createdAt) {}
    public record Page(List<Entry> items, Long nextOffset) {}
    public record Download(Entry entry, FileSystemResource resource) {}
}
