package com.trustworthyq.core.collection.documents;

import com.trustworthyq.core.collection.documents.DocumentTemplates.Template;
import com.trustworthyq.core.source.service.ImplService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class DocumentIndex {
    private final DatabaseClient database;
    private final JsonMapper json;
    private final ImplService implementations;
    private final DocumentTemplates templates;

    public Mono<Index> load(UUID owner, UUID impl, UUID run) {
        return load(owner, impl, run, false);
    }

    Mono<Index> load(UUID owner, UUID impl, UUID run, boolean includeSimilarity) {
        var lookup = database.sql("""
                SELECT r.id FROM collection_runs r
                JOIN agent_response_batches b ON b.request_id = r.id AND b.implementation_id = r.implementation_id
                WHERE r.owner_id = :owner AND r.implementation_id = :impl AND NOT r.dispatch_failed
                  AND (:run IS NULL OR r.id = :run)
                GROUP BY r.id
                HAVING count(*) FILTER (WHERE b.status = 'FAILED') = 0
                   AND max(b.batch_index) FILTER (WHERE b.status = 'COMPLETED')
                       = count(*) FILTER (WHERE b.status = 'BATCH')
                ORDER BY r.created_at DESC, r.id DESC LIMIT 1
                """).bind("owner", owner).bind("impl", impl);
        var bound = run == null ? lookup.bindNull("run", UUID.class) : lookup.bind("run", run);
        return implementations.findById(owner, impl).then(bound.map((row, ignored) -> row.get("id", UUID.class)).one())
                .switchIfEmpty(Mono.error(conflict("Выполните сбор документов для этого подключения.")))
                .flatMap(snapshot -> templates.list(owner).collectList().flatMap(rules -> database.sql("""
                        SELECT metadata->>'path' AS path, metadata->>'modificationTime' AS modified,
                               metadata->>'sha256' AS hash,
                               (CASE WHEN jsonb_typeof(metadata->'documentEvidence') = 'object' THEN
                                   CASE WHEN :similarity THEN metadata->'documentEvidence'
                                        ELSE (metadata->'documentEvidence') - 'textFingerprints' END
                                END)::text AS evidence
                        FROM visible_collection_objects WHERE request_id = :run AND owner_id = :owner
                        ORDER BY metadata->>'path'
                        """).bind("run", snapshot).bind("owner", owner).bind("similarity", includeSimilarity).map((row, ignored) -> {
                    String modified = row.get("modified", String.class);
                    String evidence = row.get("evidence", String.class);
                    return new Document(row.get("path", String.class), modified == null ? null : Instant.parse(modified),
                            evidence == null ? null : json.readValue(evidence, Evidence.class), row.get("hash", String.class));
                }).all().collectList().map(files -> build(snapshot, rules, files, Instant.now()))));
    }

    static Index build(UUID snapshot, List<Template> rules, List<Document> files, Instant now) {
        Map<String, Document> documents = new LinkedHashMap<>();
        Map<String, List<String>> names = new HashMap<>();
        for (Document file : files) {
            if (file.path() != null && documents.putIfAbsent(file.path(), file) == null) {
                names.computeIfAbsent(filename(file.path()), ignored -> new ArrayList<>()).add(file.path());
            }
        }
        Map<String, Node> nodes = new LinkedHashMap<>();
        Map<String, Set<String>> incoming = new HashMap<>();
        Set<String> protectedPaths = new LinkedHashSet<>();
        boolean complete = true;
        for (Document file : documents.values()) {
            var evidence = file.evidence();
            complete &= evidence != null && evidence.complete();
            List<Match> matches = new ArrayList<>();
            boolean pending = false;
            for (Template rule : rules) {
                if (!contains(file.path(), rule.pathContains()) || !contains(filename(file.path()), rule.nameContains())) {
                    continue;
                }
                if (!rule.contentContains().isEmpty()) {
                    if (evidence == null || !evidence.evaluatedRules().contains(rule.id())) {
                        pending = true;
                        continue;
                    }
                    if (!evidence.matchingRules().contains(rule.id())) {
                        pending |= !evidence.complete();
                        continue;
                    }
                }
                Instant until = rule.retainUntil(file.modified());
                matches.add(new Match(rule.id(), rule.name(), until, until == null || now.isBefore(until)));
            }
            Set<String> links = new LinkedHashSet<>();
            if (evidence != null) {
                for (String reference : evidence.references()) {
                    String resolved = resolve(file.path(), reference, documents, names);
                    if (resolved != null && !resolved.equals(file.path()) && links.add(resolved)) {
                        incoming.computeIfAbsent(resolved, ignored -> new LinkedHashSet<>()).add(file.path());
                    }
                }
            }
            if (pending || matches.stream().anyMatch(Match::active)) protectedPaths.add(file.path());
            Set<String> fingerprints = evidence == null || evidence.textFingerprints() == null ? Set.of() : evidence.textFingerprints();
            nodes.put(file.path(), new Node(file.path(), matches, pending, links, evidence == null ? null : evidence.eTag(), file.modified(), file.hash(), fingerprints));
        }
        Set<String> blocked = closure(protectedPaths, incoming);
        return new Index(snapshot, nodes, incoming, blocked, complete);
    }

    private static boolean contains(String value, String term) {
        return value.toLowerCase(Locale.ROOT).contains(term.toLowerCase(Locale.ROOT));
    }

    private static String filename(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static String resolve(String source, String reference, Map<String, Document> documents,
                                  Map<String, List<String>> names) {
        String target = reference.split("[?#]", 2)[0].replace('\\', '/');
        if (target.isBlank() || target.startsWith("//")) return null;
        if (documents.containsKey(target)) return target;
        if (target.contains("%")) {
            try {
                target = URLDecoder.decode(target.replace("+", "%2B"), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ignored) {
                return null;
            }
            if (documents.containsKey(target)) return target;
        }
        if (target.contains(":")) return null;
        String root = source.startsWith("file:///") ? "file:///" : source.substring(0, source.indexOf('/', 5) + 1);
        String parent = source.substring(root.length(), source.lastIndexOf('/') + 1);
        var parts = new ArrayDeque<String>();
        for (String part : (target.startsWith("/") ? target : parent + target).split("/")) {
            if (part.isEmpty() || part.equals(".")) continue;
            if (part.equals("..")) {
                if (parts.isEmpty()) return null;
                parts.removeLast();
            } else parts.addLast(part);
        }
        String candidate = root + String.join("/", parts);
        if (documents.containsKey(candidate)) return candidate;
        List<String> byName = names.get(target);
        return byName != null && byName.size() == 1 ? byName.getFirst() : null;
    }

    private static Set<String> closure(Collection<String> initial, Map<String, Set<String>> incoming) {
        Set<String> seen = new LinkedHashSet<>(initial);
        var queue = new ArrayDeque<>(initial);
        while (!queue.isEmpty()) {
            for (String parent : incoming.getOrDefault(queue.removeFirst(), Set.of())) {
                if (seen.add(parent)) queue.addLast(parent);
            }
        }
        return seen;
    }

    static ResponseStatusException conflict(String message) {
        return new ResponseStatusException(HttpStatus.CONFLICT, message);
    }

    record Document(String path, Instant modified, Evidence evidence, String hash) {}
    record Evidence(Set<String> references, Set<UUID> evaluatedRules, Set<UUID> matchingRules,
                    boolean complete, boolean text, String eTag, Set<String> textFingerprints) {
        Evidence(Set<String> references, Set<UUID> evaluatedRules, Set<UUID> matchingRules,
                 boolean complete, boolean text, String eTag) {
            this(references, evaluatedRules, matchingRules, complete, text, eTag, Set.of());
        }
    }
    record Node(String path, List<Match> matches, boolean pending, Set<String> links, String eTag,
                Instant modified, String hash, Set<String> fingerprints) {}
    public record Match(UUID id, String name, Instant retainUntil, boolean active) {}
    public record Item(String path, List<Match> templates, boolean needsScan, boolean deleteBlocked,
                       Set<String> references, Set<String> referencedBy, List<Related> related) {}
    public record Related(String path, String kind, int sharedFragments) {}
    public record Page(UUID runId, List<Item> items, Long nextOffset, boolean complete) {}

    public record Index(UUID runId, Map<String, Node> nodes, Map<String, Set<String>> incoming,
                        Set<String> blocked, boolean complete) {
        public Page page(String search, String mode, long offset, int requestedLimit) {
            int limit = Math.clamp(requestedLimit, 1, 100);
            long start = Math.max(0, offset);
            boolean graph = "graph".equals(mode) || "linked".equals(mode);
            var matching = nodes.values().stream().filter(node -> contains(node.path(), search))
                    .filter(node -> !"matched".equals(mode) || node.pending() || !node.matches().isEmpty())
                    .filter(node -> !"linked".equals(mode) || !node.links().isEmpty() || incoming.containsKey(node.path()));
            if ("graph".equals(mode)) {
                matching = matching.sorted(Comparator.comparingInt(node ->
                        !node.links().isEmpty() || incoming.containsKey(node.path()) ? 0 : !node.fingerprints().isEmpty() ? 1 : 2));
            }
            var selected = matching.skip(start).limit(limit + 1L).toList();
            RelatedDocuments related = graph && !selected.isEmpty() ? new RelatedDocuments(nodes.values(), selected) : null;
            var items = selected.stream()
                    .map(node -> {
                        Set<String> inbound = incoming.getOrDefault(node.path(), Set.of());
                        List<Related> suggestions = List.of();
                        if (related != null) {
                            var direct = new HashSet<>(node.links());
                            direct.addAll(inbound);
                            suggestions = related.find(node, direct);
                        }
                        return new Item(node.path(), node.matches(), node.pending(), blocked.contains(node.path()),
                                node.links(), inbound, suggestions);
                    }).toList();
            return new Page(runId, items.stream().limit(limit).toList(), items.size() > limit ? start + limit : null, complete);
        }

        public void requireArchivable(String path, String eTag) {
            checkRetention(requireCurrent(path, eTag));
        }

        public FileVersion fileVersionForArchive(String path) {
            Node node = nodes.get(path);
            if (node == null || node.hash() == null || node.modified() == null) {
                throw conflict("Файл отсутствует в последнем сборе. Запустите сбор заново.");
            }
            checkRetention(node);
            return new FileVersion(node.hash(), node.modified());
        }

        private void checkRetention(Node node) {
            if (node.pending() || node.matches().stream().anyMatch(Match::active)) {
                throw conflict("Документ защищён сроком хранения или ожидает проверки по шаблону.");
            }
        }

        public Set<String> deletionPaths(String path, String eTag, boolean cascade) {
            requireCurrent(path, eTag);
            if (!complete) throw conflict("Сбор ссылок неполный. Удаление заблокировано.");
            Set<String> related = cascade ? closure(List.of(path), incoming) : Set.of(path);
            if (related.stream().anyMatch(blocked::contains)) {
                throw conflict("Срок хранения блокирует удаление документа или связанных документов.");
            }
            return related;
        }

        public Instant scannedModification(String path, String eTag) {
            return requireCurrent(path, eTag).modified();
        }

        private Node requireCurrent(String path, String eTag) {
            Node node = nodes.get(path);
            if (node == null || node.eTag() == null || !Objects.equals(node.eTag(), eTag)) {
                throw conflict("Документ изменился или отсутствует в последнем сборе. Запустите сбор заново.");
            }
            return node;
        }
    }

    public record FileVersion(String sha256, Instant modified) {}
}
