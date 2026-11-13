package com.trustworthyq.core.collection.controller;

import com.trustworthyq.core.collection.service.CollectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/collect")
@RequiredArgsConstructor
public class CollectController {

    private final CollectionService collectionService;

    @PostMapping("/{id}/run")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> runCollection(@PathVariable UUID id) {
        return collectionService.runCollection(id);
    }
}