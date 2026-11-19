package com.trustworthyq.core.source.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.s3.S3Operations.ArchiveConfirmation;
import com.trustworthyq.core.source.s3.S3Operations.ObjectLocation;
import com.trustworthyq.core.source.service.ImplService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class S3OperationsTest {
    private final UUID owner = UUID.randomUUID();
    private final UUID implId = UUID.randomUUID();
    private final ConcurrentLinkedQueue<String> requests = new ConcurrentLinkedQueue<>();
    private final AtomicReference<String> eTag = new AtomicReference<>("\"original\"");
    private final AtomicReference<String> archiveETag = new AtomicReference<>();
    private final AtomicBoolean rejectArchiveDelete = new AtomicBoolean();
    private HttpServer server;
    private S3Operations operations;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::respond);
        server.start();
        var mapper = JsonMapper.builder().build();
        var implementations = mock(ImplService.class);
        when(implementations.findById(owner, implId)).thenReturn(Mono.just(ImplResponse.builder()
                .sourceId(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14"))
                .config(mapper.writeValueAsString(Map.of("endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                        "bucket", "documents", "prefix", "reports/", "archiveBucket", "archive",
                        "accessKey", "fixture", "secretKey", "fixture")))
                .build()));
        operations = new S3Operations(implementations, mapper);
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void copiesOnlyAfterConfirmationAndRejectsStalePreview() {
        var source = new ObjectLocation("documents", "reports/file.txt");
        var preview = operations.archivePreview(owner, implId, source).block();
        assertThat(requests).containsExactly("HEAD /documents/reports/file.txt");
        assertThatThrownBy(() -> operations.archive(owner, implId, new ArchiveConfirmation(false, preview)).block())
                .isInstanceOf(ResponseStatusException.class);
        eTag.set("\"changed\"");
        assertThatThrownBy(() -> operations.archive(owner, implId, new ArchiveConfirmation(true, preview)).block())
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("409");
        assertThat(requests).allMatch(request -> request.startsWith("HEAD"));
        var current = operations.archivePreview(owner, implId, source).block();
        var result = operations.archive(owner, implId, new ArchiveConfirmation(true, current)).block();
        assertThat(result.state()).isEqualTo("ARCHIVED");
        assertThat(result.sourceDeleted()).isFalse();
        assertThat(result.destination().bucket()).isEqualTo("archive");
        assertThat(requests.stream().reduce((first, last) -> last).orElseThrow()).startsWith("PUT /archive/dacs-archive/").endsWith("match=\"changed\" destination=*");
        assertThat(requests).noneMatch(request -> request.startsWith("DELETE"));
    }

    @Test
    void restoresMissingOriginalAndOnlyUndoesWhileRestoredObjectStillExists() {
        var source = new ObjectLocation("documents", "reports/file.txt");
        var preview = operations.archivePreview(owner, implId, source).block();
        var archived = operations.archive(owner, implId, new ArchiveConfirmation(true, preview)).block().details();
        assertThat(operations.restore(owner, implId, archived).block()).isEqualTo(archived.original());

        eTag.set("\"replacement\"");
        assertThatThrownBy(() -> operations.restore(owner, implId, archived).block())
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("409");
        eTag.set(null);
        var restored = archived.restoredAs(operations.restore(owner, implId, archived).block());
        assertThat(requests).anyMatch(value -> value.startsWith("PUT /documents/reports/file.txt")
                && value.endsWith("match=\"copied\" destination=*"));
        operations.verifyUndo(owner, implId, restored).block();
        eTag.set(null);
        assertThatThrownBy(() -> operations.verifyUndo(owner, implId, restored).block())
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("409");
        assertThat(requests).noneMatch(value -> value.startsWith("DELETE"));
    }

    @Test
    void deletesMatchingOriginalAndArchiveAndToleratesRetries() {
        var source = new ObjectLocation("documents", "reports/file.txt");
        var preview = operations.archivePreview(owner, implId, source).block();
        var archived = operations.archive(owner, implId, new ArchiveConfirmation(true, preview)).block().details();
        archiveETag.set("\"replacement\"");
        assertThatThrownBy(() -> operations.deleteArchive(owner, implId, archived).block())
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("409");
        archiveETag.set("\"copied\"");
        eTag.set("\"replacement\"");
        assertThatThrownBy(() -> operations.deleteArchive(owner, implId, archived).block())
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("409");
        assertThat(requests).noneMatch(value -> value.startsWith("DELETE"));
        eTag.set("\"original\"");
        rejectArchiveDelete.set(true);
        assertThatThrownBy(() -> operations.deleteArchive(owner, implId, archived).block())
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("403");
        assertThat(eTag.get()).isNull();
        assertThat(archiveETag.get()).isEqualTo("\"copied\"");
        operations.deleteArchive(owner, implId, archived).block();
        operations.deleteArchive(owner, implId, archived).block();
        String archiveDelete = "DELETE /archive/" + archived.destination().key() + " match=\"copied\"";
        assertThat(requests.stream().filter(value -> value.startsWith("DELETE")).toList())
                .containsExactly("DELETE /documents/reports/file.txt match=\"original\"", archiveDelete, archiveDelete);
        assertThat(archiveETag.get()).isNull();
    }


    private void respond(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String uri = exchange.getRequestURI().toString();
            if ("HEAD".equals(method)) {
                requests.add(method + " " + uri);
                String tag = uri.startsWith("/archive/") ? archiveETag.get() : eTag.get();
                if (tag == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                exchange.getResponseHeaders().set("ETag", tag);
                exchange.getResponseHeaders().set("Content-Length", "4");
                exchange.getResponseHeaders().set("Last-Modified", "Sat, 01 Nov 2025 12:00:00 GMT");
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            if ("DELETE".equals(method)) {
                requests.add(method + " " + uri + " match=" + exchange.getRequestHeaders().getFirst("If-Match"));
                if (uri.startsWith("/archive/")) {
                    if (rejectArchiveDelete.getAndSet(false)) {
                        exchange.sendResponseHeaders(403, -1);
                        return;
                    }
                    archiveETag.set(null);
                } else {
                    eTag.set(null);
                }
                exchange.sendResponseHeaders(204, -1);
                return;
            }
            String body;
            if ("PUT".equals(method)) {
                requests.add(method + " " + uri + " match=" + exchange.getRequestHeaders().getFirst("x-amz-copy-source-if-match") + " destination=" + exchange.getRequestHeaders().getFirst("If-None-Match"));
                if (uri.startsWith("/archive/")) {
                    archiveETag.set("\"copied\"");
                } else {
                    eTag.set("\"copied\"");
                }
                body = "<CopyObjectResult><ETag>\"copied\"</ETag></CopyObjectResult>";
            } else {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        }
    }
}
