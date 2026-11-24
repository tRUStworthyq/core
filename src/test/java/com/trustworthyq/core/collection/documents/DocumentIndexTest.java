package com.trustworthyq.core.collection.documents;

import com.trustworthyq.core.collection.documents.DocumentIndex.Document;
import com.trustworthyq.core.collection.documents.DocumentIndex.Evidence;
import com.trustworthyq.core.collection.documents.DocumentTemplates.Template;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentIndexTest {
    private static final Instant NOW = Instant.parse("2026-11-23T10:00:00Z");
    private static final String CONTRACT = "s3://docs/contracts/agreement.pdf";
    private static final String REPORT = "s3://docs/reports/report.md";
    private static final String README = "s3://docs/readme.txt";
    private static final UUID RUN = UUID.randomUUID();

    @Test
    void retentionPropagatesTransitivelyAndBlocksArchiveOfMatchedDocument() {
        var rule = new Template(UUID.randomUUID(), "Договор", "/contracts/", ".pdf", "", 365);
        var index = DocumentIndex.build(RUN, List.of(rule), files(), NOW);

        assertThat(index.blocked()).containsExactlyInAnyOrder(CONTRACT, REPORT, README);
        assertThatThrownBy(() -> index.requireArchivable(CONTRACT, "etag")).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> index.deletionPaths(README, "etag", false)).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void cascadeIncludesIncomingReferencesAndRequiresExplicitMode() {
        var index = DocumentIndex.build(RUN, List.of(), files(), NOW);

        assertThat(index.deletionPaths(CONTRACT, "etag", false)).containsExactly(CONTRACT);
        assertThat(index.deletionPaths(CONTRACT, "etag", true)).containsExactlyInAnyOrder(CONTRACT, REPORT, README);
        assertThatThrownBy(() -> index.deletionPaths(CONTRACT, "changed", true)).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void newContentRuleRequiresFreshEvidenceBeforeArchive() {
        var rule = new Template(UUID.randomUUID(), "Договор", "/contracts/", ".pdf", "signed", 365);
        var index = DocumentIndex.build(RUN, List.of(rule), files(), NOW);

        assertThat(index.nodes().get(CONTRACT).pending()).isTrue();
        assertThatThrownBy(() -> index.requireArchivable(CONTRACT, "etag")).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void calendarYearsAccountForLeapDay() {
        var rule = new Template(UUID.randomUUID(), "Договор", "/contracts/", "", "", null, 1);
        var modified = Instant.parse("2024-02-29T12:00:00Z");
        var index = DocumentIndex.build(RUN, List.of(rule),
                List.of(new Document(CONTRACT, modified, null, null)), Instant.parse("2025-02-28T11:00:00Z"));

        assertThat(index.nodes().get(CONTRACT).matches()).singleElement().satisfies(match -> {
            assertThat(match.retainUntil()).isEqualTo(Instant.parse("2025-02-28T12:00:00Z"));
            assertThat(match.active()).isTrue();
        });
    }

    @Test
    void findsSharedIdentifiersAndFragmentsWithoutExtendingDeletionRules() {
        var protectedTemplate = new Template(UUID.randomUUID(), "Договор", "/contracts/", "", "", 365);
        var index = DocumentIndex.build(RUN, List.of(protectedTemplate), List.of(
                relatedFile(CONTRACT, "abc", "1", "2", "3", "4", "id:project"),
                relatedFile(REPORT, "different", "1", "2", "3", "4"),
                relatedFile(README, "another", "id:project"),
                relatedFile("s3://docs/copy.txt", "abc")
        ), NOW);

        var result = index.page(CONTRACT, "graph", 0, 5).items().getFirst();
        assertThat(result.related()).extracting(DocumentIndex.Related::kind)
                .containsExactly("IDENTICAL_CONTENT", "SHARED_IDENTIFIERS", "SHARED_FRAGMENTS");
        assertThat(index.blocked()).containsExactly(CONTRACT);
        assertThat(index.deletionPaths(REPORT, "etag", true)).containsExactly(REPORT);
    }

    private Document relatedFile(String path, String sha256, String... hashes) {
        return new Document(path, NOW.minusSeconds(60),
                new Evidence(Set.of(), Set.of(), Set.of(), true, true, "etag", Set.of(hashes)), sha256);
    }

    private List<Document> files() {
        return List.of(file(CONTRACT), file(REPORT, "../contracts/agreement.pdf"), file(README, "report.md"));
    }

    private Document file(String path, String... links) {
        return new Document(path, NOW.minusSeconds(60), new Evidence(Set.of(links), Set.of(), Set.of(), true, true, "etag"), "hash");
    }
}
