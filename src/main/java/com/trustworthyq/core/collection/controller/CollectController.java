package com.trustworthyq.core.collection.controller;

import com.trustworthyq.core.collection.repository.AgentResponseRepository.RunStatus;
import com.trustworthyq.core.collection.service.CollectionService;
import com.trustworthyq.core.collection.scoring.AgingCalculator.Assessment;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/collect")
@RequiredArgsConstructor
public class CollectController {
    private final CollectionService collectionService;

    @PostMapping("/{id}/run")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<RunStatus> runCollection(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return collectionService.runCollection(UUID.fromString(jwt.getSubject()), id);
    }

    @GetMapping("/runs/{requestId}")
    public Mono<RunStatus> status(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID requestId) {
        return collectionService.status(UUID.fromString(jwt.getSubject()), requestId);
    }

    @GetMapping("/runs/{requestId}/objects")
    public Mono<List<JsonNode>> objects(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID requestId,
                                       @RequestParam(defaultValue = "0") long offset,
                                       @RequestParam(defaultValue = "50") int limit,
                                       @RequestParam(required = false) String piiStatus) {
        return collectionService.objects(UUID.fromString(jwt.getSubject()), requestId, offset, limit, piiStatus);
    }

    @GetMapping("/runs/{requestId}/aging")
    public Mono<List<Assessment>> aging(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID requestId,
                                        @RequestParam(defaultValue = "0") long offset,
                                        @RequestParam(defaultValue = "50") int limit) {
        return collectionService.assess(UUID.fromString(jwt.getSubject()), requestId, offset, limit);
    }
}