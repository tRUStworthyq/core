package com.trustworthyq.core;

import com.trustworthyq.core.collection.documents.DocumentIndex;
import com.trustworthyq.core.collection.documents.DocumentTemplates;
import com.trustworthyq.core.collection.documents.FilesystemArchive;
import com.trustworthyq.core.collection.repository.AgentResponseRepository;
import com.trustworthyq.core.collection.service.CollectionService;
import com.trustworthyq.core.source.s3.S3Archive;
import com.trustworthyq.core.source.service.ImplService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class CoreApplicationTests {
    private static final Duration WAIT = Duration.ofSeconds(10);

    @Autowired DatabaseClient database;
    @Autowired AgentResponseRepository responses;
    @Autowired DocumentTemplates templates;
    @Autowired DocumentIndex documents;
    @Autowired CollectionService collections;
    @Autowired S3Archive archive;
    @Autowired ImplService implementations;
    @Autowired TransactionalOperator transaction;
    @Autowired JsonMapper json;

    @Test
    void contextLoads() {
    }

    @Test
    void matchesSavedTemplateInLatestRunAndFiltersRecommendationsBeforePaging() {
        UUID owner = UUID.randomUUID();
        UUID impl = UUID.randomUUID();
        UUID run = UUID.randomUUID();
        database.sql("""
                INSERT INTO impl (id, owner_id, name, config, state, source_id)
                VALUES (:id, :owner, 'Fixture documents', '{}'::jsonb, 'NOT_RUN',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14')
                """).bind("id", impl).bind("owner", owner).fetch().rowsUpdated().block(WAIT);
        var template = templates.save(owner, null,
                new DocumentTemplates.Template(null, "Договор", "/contracts/", "", "", 365)).block(WAIT);
        assertThat(template).isNotNull();
        var edited = templates.save(owner, template.id(),
                new DocumentTemplates.Template(null, "Договоры", "/contracts/", "", "", null, 2)).block(WAIT);
        assertThat(edited).isNotNull();
        assertThat(edited.id()).isEqualTo(template.id());
        assertThat(templates.list(owner).collectList().block(WAIT)).containsExactly(edited);
        responses.start(run, owner, impl).then(responses.save("""
                {"requestId":"%s","id":"%s","batchIndex":0,"status":"BATCH","items":[
                  {"path":"s3://docs/contracts/a.pdf","pii":{"status":"REVIEW_REQUIRED"},
                   "accessStatistics":{"lastReadTime":"%s"},
                   "documentEvidence":{"references":[],"evaluatedRules":[],"matchingRules":[],"complete":true,"text":true,"eTag":"abc"}},
                  {"path":"s3://docs/no-usage.txt",
                   "documentEvidence":{"references":[],"evaluatedRules":[],"matchingRules":[],"complete":true,"text":true}},
                  {"path":"s3://docs/report.md","accessStatistics":{"lastReadTime":"%s"},
                   "documentEvidence":{"references":["contracts/a.pdf"],
                   "evaluatedRules":[],"matchingRules":[],"complete":true,"text":true,"eTag":"def"}},
                  {"path":"s3://docs/reviewed.md","accessStatistics":{"lastReadTime":"%s"},
                   "documentEvidence":{"references":[],"evaluatedRules":[],"matchingRules":[],"complete":true,"text":true}}]}
                """.formatted(run, impl, Instant.now().minusSeconds(60), Instant.now().minusSeconds(60),
                Instant.now().minusSeconds(60)))).then(responses.save("""
                {"requestId":"%s","id":"%s","batchIndex":1,"status":"COMPLETED","items":[]}
                """.formatted(run, impl))).block(WAIT);

        var index = documents.load(owner, impl, null).block(WAIT);
        assertThat(index).isNotNull();
        assertThat(index.blocked()).containsExactlyInAnyOrder("s3://docs/contracts/a.pdf", "s3://docs/report.md");
        assertThat(collections.recommendations(owner, run, 0, 1).block(WAIT))
                .singleElement().satisfies(score -> assertThat(score.path()).isEqualTo("s3://docs/report.md"));
        assertThat(collections.recommendations(owner, run, 1, 1).block(WAIT))
                .singleElement().satisfies(score -> assertThat(score.path()).isEqualTo("s3://docs/reviewed.md"));

        UUID contractEntry = archiveFixture(owner, impl, "contracts/a.pdf", "abc");
        UUID reportEntry = archiveFixture(owner, impl, "report.md", "def");
        assertThatThrownBy(() -> archive.deletePlan(owner, impl, reportEntry, false).block(WAIT))
                .hasMessageContaining("Срок хранения");

        templates.delete(owner, template.id()).block(WAIT);
        assertThat(templates.list(owner).collectList().block(WAIT)).isEmpty();
        var single = archive.deletePlan(owner, impl, contractEntry, false).block(WAIT);
        assertThat(single).isNotNull();
        assertThat(single.ids()).containsExactly(contractEntry);
        var plan = archive.deletePlan(owner, impl, contractEntry, true).block(WAIT);
        assertThat(plan).isNotNull();
        assertThat(plan.ids()).containsExactly(reportEntry, contractEntry);
    }

    private UUID archiveFixture(UUID owner, UUID impl, String key, String eTag) {
        UUID id = UUID.randomUUID();
        database.sql("""
                INSERT INTO s3_archive (id, owner_id, impl_id, state, payload)
                VALUES (:id, :owner, :impl, 'ARCHIVED', CAST(:payload AS jsonb))
                """).bind("id", id).bind("owner", owner).bind("impl", impl)
                .bind("payload", """
                        {"source":{"bucket":"docs","key":"%s"}, "destination":{"bucket":"archive","key":"%s"},
                         "sizeBytes":1,"storageId":"fixture","original":{"eTag":"%s"},"archive":{"eTag":"%s"}}
                        """.formatted(key, key, eTag, eTag)).fetch().rowsUpdated().block(WAIT);
        return id;
    }

    @Test
    void archivesFilesystemCopiesFromCompletedCollection(@TempDir Path source, @TempDir Path destination) throws Exception {
        UUID owner = UUID.randomUUID();
        UUID impl = UUID.randomUUID();
        UUID run = UUID.randomUUID();
        Path file = Files.writeString(source.resolve("report.txt"), "fixture");
        var modified = Files.getLastModifiedTime(file).toInstant();
        var hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        database.sql("""
                INSERT INTO impl (id, owner_id, name, config, state, source_id)
                VALUES (:id, :owner, 'Fixture filesystem', '{"directory":"."}'::jsonb, 'NOT_RUN',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15')
                """).bind("id", impl).bind("owner", owner).fetch().rowsUpdated().block(WAIT);
        responses.start(run, owner, impl).then(responses.save("""
                {"requestId":"%s","id":"%s","batchIndex":0,"status":"BATCH","items":[
                  {"path":"file:///report.txt","modificationTime":"%s","sha256":"%s",
                   "documentEvidence":{"references":[],"evaluatedRules":[],"matchingRules":[],"complete":true,"text":true}},
                  {"path":"file:///index.md",
                   "documentEvidence":{"references":["report.txt"],"evaluatedRules":[],"matchingRules":[],"complete":true,"text":true}}]}
                """.formatted(run, impl, modified, hash))).then(responses.save("""
                {"requestId":"%s","id":"%s","batchIndex":1,"status":"COMPLETED","items":[]}
                """.formatted(run, impl))).block(WAIT);
        var graph = documents.load(owner, impl, null).block(WAIT);
        assertThat(graph).isNotNull();
        assertThat(graph.page("report.txt", "graph", 0, 10).items()).singleElement()
                .satisfies(item -> assertThat(item.referencedBy()).containsExactly("file:///index.md"));
        var files = new FilesystemArchive(documents, database, implementations, transaction, json,
                source.toString(), destination.toString());
        String path = "file:///report.txt";
        var entry = files.create(owner, impl, path).block(WAIT);
        assertThat(entry).isNotNull();
        assertThat(files.create(owner, impl, path).block(WAIT)).isEqualTo(entry);
        var page = files.page(owner, impl, 0, 25).block(WAIT);
        assertThat(page).isNotNull();
        assertThat(page.items()).containsExactly(entry);
        var download = files.download(owner, impl, entry.id()).block(WAIT);
        assertThat(download).isNotNull();
        assertThat(Files.readString(download.resource().getFile().toPath())).isEqualTo("fixture");
        assertThat(Files.readString(file)).isEqualTo("fixture");

        templates.save(owner, null, new DocumentTemplates.Template(null, "Отчёт", "", "report", "", 365)).block(WAIT);
        assertThatThrownBy(() -> files.create(owner, impl, path).block(WAIT)).hasMessageContaining("сроком хранения");
    }
}
