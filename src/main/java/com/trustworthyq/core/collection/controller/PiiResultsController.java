package com.trustworthyq.core.collection.controller;

import com.trustworthyq.core.collection.pii.PiiResults;
import com.trustworthyq.core.collection.pii.PiiResults.Category;
import com.trustworthyq.core.collection.pii.PiiResults.Filter;
import com.trustworthyq.core.collection.pii.PiiResults.Page;
import com.trustworthyq.core.collection.pii.PiiResults.Summary;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/collect/runs/{requestId}/pii")
@RequiredArgsConstructor
public class PiiResultsController {
    private final PiiResults results;

    @GetMapping("/summary")
    public Mono<Summary> summary(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID requestId) {
        return results.summary(UUID.fromString(jwt.getSubject()), requestId);
    }

    @GetMapping("/files")
    public Mono<Page> files(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID requestId,
                            @RequestParam(defaultValue = "FINDINGS") Filter status,
                            @RequestParam(required = false) Category category,
                            @RequestParam(defaultValue = "0") long offset,
                            @RequestParam(defaultValue = "50") int limit) {
        return results.files(UUID.fromString(jwt.getSubject()), requestId, status, category, offset, limit);
    }
}
