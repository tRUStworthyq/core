package com.trustworthyq.core.collection.scoring;

import com.trustworthyq.core.collection.repository.AgentResponseRepository.ScoringObject;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@Component
public class AgingCalculator {
    private final AgingRules rules;

    public AgingCalculator(AgingRules rules) {
        this.rules = rules;
    }

    public Assessment assess(ScoringObject input) {
        JsonNode object = input.object();
        String path = object.path("path").asString("");
        int slash = path.lastIndexOf('/');
        String name = path.substring(slash + 1).toLowerCase(Locale.ROOT);
        String prefix = path.startsWith("s3://") ? path.substring(5) : path;
        String[] folders = prefix.substring(0, Math.max(0, prefix.lastIndexOf('/'))).split("/");
        List<Signal> signals = new ArrayList<>();
        Instant modified = instant(object.path("modificationTime"));
        add(signals, input.newerDuplicate(), "NEWER_EXACT_DUPLICATE", rules.duplicateWeight());
        add(signals, rules.nameMarkers().stream().anyMatch(name::contains), "NAME_MARKER", rules.nameWeight());
        add(signals, Arrays.stream(folders).skip(1).map(f -> f.toLowerCase(Locale.ROOT))
                .anyMatch(rules.folderMarkers()::contains), "FOLDER_MARKER", rules.folderWeight());
        JsonNode ownerActive = object.path("ownerActive");
        add(signals, ownerActive.isBoolean() && !ownerActive.asBoolean(), "INACTIVE_OWNER", rules.ownerWeight());
        add(signals, modified != null && modified.isBefore(input.asOf().atOffset(ZoneOffset.UTC)
                .minusYears(rules.oldYears()).toInstant()), "OLD_MODIFICATION", rules.oldWeight());
        JsonNode size = object.path("sizeBytes");
        add(signals, size.isNumber() && size.asLong() <= rules.smallBytes(), "SMALL_FILE", rules.smallWeight());

        double b = Math.min(1, signals.stream().mapToDouble(Signal::weight).sum());
        Usage usage = usage(object.path("accessStatistics"), input.asOf());
        Double s = usage.value() == null ? null : 100 * usage.value() * (0.6 + 0.4 * b);
        String status = s == null ? "INSUFFICIENT_DATA"
                : s >= rules.obsoleteScore() ? "OBSOLETE" : s >= rules.coolingScore() ? "COOLING" : "ACTIVE";
        String recommendation = switch (status) {
            case "OBSOLETE" -> "REVIEW_FOR_ARCHIVE";
            case "ACTIVE" -> "KEEP";
            case "COOLING" -> "REVIEW_USAGE";
            default -> "COLLECT_ACCESS_HISTORY";
        };
        String pii = object.path("pii").path("status").asString("");
        boolean needsReview = "DETECTED".equals(pii) || "REVIEW_REQUIRED".equals(pii)
                || "PARTIAL".equals(pii) || "ERROR".equals(pii);
        ResponseState responseState = needsReview ? ResponseState.WARNING : switch (status) {
            case "ACTIVE" -> ResponseState.KEEP;
            case "OBSOLETE" -> ResponseState.ARCHIVE_CONFIRMATION_REQUIRED;
            default -> ResponseState.WARNING;
        };
        return new Assessment(path, usage.value(), b, s, status, usage.reason(), signals,
                input.preferredPath(), recommendation, responseState, pii.isEmpty() ? null : pii);
    }

    private Usage usage(JsonNode audit, Instant asOf) {
        var lastRead = instant(audit.path("lastReadTime"));
        var start = instant(audit.path("coverageStart"));
        var end = instant(audit.path("coverageEnd"));
        if (lastRead != null && !lastRead.isAfter(asOf)
                && !lastRead.isBefore(asOf.minus(Duration.ofDays(rules.recentDays())))) {
            return new Usage(rules.recentUsage(), "RECENT_READ");
        }
        if (start == null || end == null || !start.isBefore(end) || end.isAfter(asOf)
                || end.isBefore(asOf.minus(Duration.ofDays(rules.auditLagDays())))) {
            return new Usage(null, "AUDIT_COVERAGE_UNVERIFIED");
        }
        Instant inactiveSince = lastRead == null || lastRead.isBefore(start) ? start : lastRead;
        if (inactiveSince.isBefore(end.minus(Duration.ofDays(rules.unusedDays())))) {
            return new Usage(rules.unusedUsage(), "NO_READS_OVER_UNUSED_WINDOW");
        }
        if (!inactiveSince.isAfter(end.minus(Duration.ofDays(rules.coolingDays())))
                && (lastRead != null || !start.isAfter(end.minus(Duration.ofDays(rules.coolingDays()))))) {
            return new Usage(rules.coolingUsage(), "NO_READS_OVER_COOLING_WINDOW");
        }
        return new Usage(null, "NO_APPLICABLE_USAGE_RULE");
    }

    private static void add(List<Signal> signals, boolean applies, String rule, double weight) {
        if (applies) {
            signals.add(new Signal(rule, weight));
        }
    }

    private static Instant instant(JsonNode node) {
        if (!node.isString()) {
            return null;
        }
        try {
            return Instant.parse(node.asString());
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private record Usage(Double value, String reason) {
    }

    public record Signal(String rule, double weight) {
    }

    public enum ResponseState {
        KEEP, WARNING, ARCHIVE_CONFIRMATION_REQUIRED
    }

    public record Assessment(String path, Double u, double b, Double s, String status,
                             String usageReason, List<Signal> signals, String preferredPath,
                             String recommendation, ResponseState responseState, String piiStatus) {
    }
}
