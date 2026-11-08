package com.trustworthyq.core.source.controller;

import com.trustworthyq.core.source.dto.SourceResponse;
import com.trustworthyq.core.source.service.SourceService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/sources")
@RequiredArgsConstructor
public class SourceController {

    private final SourceService sourceService;

    @GetMapping
    public Flux<SourceResponse> getAll() {
        return sourceService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<SourceResponse> getById(@PathVariable UUID id) {
        return sourceService.findById(id);
    }
}