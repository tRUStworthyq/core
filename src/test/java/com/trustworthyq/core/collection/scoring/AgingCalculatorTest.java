package com.trustworthyq.core.collection.scoring;

import com.trustworthyq.core.collection.repository.AgentResponseRepository.ScoringObject;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class AgingCalculatorTest {
    private static final Instant AS_OF = Instant.parse("2026-11-19T00:00:00Z");
    private final AgingCalculator calculator = new AgingCalculator(new AgingRules(
            30, 90, 180, 1, 0.1, 0.7, 1, 3, 32, 0.35, 0.15, 0.15, 0.20, 0.10, 0.05,
            30, 60, List.of("copy"), List.of("archive")));
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void doesNotInferInactivityFromZeroReadsWithoutCoverage() {
        var result = assess("""
                {"accessStatistics":{"successfulReads":0}}
                """);
        assertThat(result.u()).isNull();
        assertThat(result.s()).isNull();
        assertThat(result.status()).isEqualTo("INSUFFICIENT_DATA");
        assertThat(result.responseState()).isEqualTo(AgingCalculator.ResponseState.WARNING);
    }

    @Test
    void explainsWeightsForAnUnusedDuplicate() {
        var result = assess("""
                {"path":"s3://docs/archive/report_copy.txt","sizeBytes":10,
                 "accessStatistics":{"successfulReads":0,"coverageStart":"2026-01-01T00:00:00Z",
                                     "coverageEnd":"2026-11-19T00:00:00Z"}}
                """);
        assertThat(result.u()).isEqualTo(1.0);
        assertThat(result.b()).isCloseTo(0.70, within(0.00001));
        assertThat(result.s()).isCloseTo(88.0, within(0.00001));
        assertThat(result.status()).isEqualTo("OBSOLETE");
        assertThat(result.responseState()).isEqualTo(AgingCalculator.ResponseState.ARCHIVE_CONFIRMATION_REQUIRED);
        assertThat(result.signals()).extracting(AgingCalculator.Signal::rule)
                .containsExactly("NEWER_EXACT_DUPLICATE", "NAME_MARKER", "FOLDER_MARKER", "SMALL_FILE");
    }

    @Test
    void recognizesRecentReadWithoutInferringMissingHistory() {
        var result = assess("""
                {"accessStatistics":{"successfulReads":1,"lastReadTime":"2026-11-01T00:00:00Z"}}
                """);
        assertThat(result.u()).isEqualTo(0.1);
        assertThat(result.status()).isEqualTo("ACTIVE");
        assertThat(result.responseState()).isEqualTo(AgingCalculator.ResponseState.KEEP);
    }

    private AgingCalculator.Assessment assess(String object) {
        return calculator.assess(new ScoringObject(mapper.readTree(object), AS_OF, true, "preferred.txt"));
    }
}
