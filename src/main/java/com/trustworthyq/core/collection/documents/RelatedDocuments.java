package com.trustworthyq.core.collection.documents;

import com.trustworthyq.core.collection.documents.DocumentIndex.Node;
import com.trustworthyq.core.collection.documents.DocumentIndex.Related;

import java.util.*;

final class RelatedDocuments {
    private static final int MAX_POSTINGS = 32;
    private static final int MAX_RELATED = 12;
    private static final String EMPTY_FILE_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private final Map<String, List<Node>> copies = new HashMap<>();
    private final Map<String, List<Node>> phrases = new HashMap<>();

    RelatedDocuments(Collection<Node> nodes, Collection<Node> selected) {
        for (Node node : selected) {
            if (node.hash() != null && !node.hash().equals(EMPTY_FILE_HASH)) {
                copies.putIfAbsent(node.hash(), new ArrayList<>());
            }
            node.fingerprints().forEach(hash -> phrases.putIfAbsent(hash, new ArrayList<>()));
        }
        for (Node node : nodes) {
            index(copies, node.hash(), node);
            node.fingerprints().forEach(hash -> index(phrases, hash, node));
        }
    }

    private static void index(Map<String, List<Node>> index, String key, Node node) {
        var postings = index.get(key);
        if (postings != null && postings.size() <= MAX_POSTINGS) postings.add(node);
    }

    List<Related> find(Node source, Set<String> direct) {
        Map<String, Related> results = new LinkedHashMap<>();
        for (Node copy : copies.getOrDefault(source.hash(), List.of())) {
            if (!copy.path().equals(source.path()) && !direct.contains(copy.path())) {
                results.put(copy.path(), new Related(copy.path(), "IDENTICAL_CONTENT", 0));
            }
        }
        Map<Node, Integer> shared = new IdentityHashMap<>();
        Map<Node, Integer> identifiers = new IdentityHashMap<>();
        for (String hash : source.fingerprints()) {
            List<Node> postings = phrases.get(hash);
            if (postings.size() > MAX_POSTINGS) continue;
            for (Node target : postings) {
                if (!target.path().equals(source.path()) && !direct.contains(target.path())
                        && !results.containsKey(target.path())) {
                    var matches = hash.startsWith("id:") ? identifiers : shared;
                    matches.merge(target, 1, Integer::sum);
                }
            }
        }
        identifiers.forEach((target, count) -> results.put(target.path(), new Related(target.path(), "SHARED_IDENTIFIERS", count)));
        shared.forEach((target, count) -> {
            long smaller = Math.min(phraseCount(source), phraseCount(target));
            if (count >= 4 && count * 100 >= smaller * 20) {
                results.putIfAbsent(target.path(), new Related(target.path(), "SHARED_FRAGMENTS", count));
            }
        });
        return results.values().stream().sorted(Comparator
                .comparingInt((Related link) -> switch (link.kind()) {
                    case "IDENTICAL_CONTENT" -> 0;
                    case "SHARED_IDENTIFIERS" -> 1;
                    default -> 2;
                })
                .thenComparing(Comparator.comparingInt(Related::sharedFragments).reversed())
                .thenComparing(Related::path)).limit(MAX_RELATED).toList();
    }

    private static long phraseCount(Node node) {
        return node.fingerprints().stream().filter(hash -> !hash.startsWith("id:")).count();
    }
}
