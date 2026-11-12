package com.trustworthyq.core.source.controller;

import com.trustworthyq.core.source.dto.SourceResponse;
import com.trustworthyq.core.source.service.SourceService;
import com.trustworthyq.core.source.service.impl.SourceServiceImpl;
import com.trustworthyq.core.source.utils.PageableFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/sources")
@RequiredArgsConstructor
public class SourceController {

    private final SourceService sourceService;
    private final PageableFactory pageableFactory;

    @GetMapping
    public Mono<Page<SourceResponse>> getAll(@RequestParam(required = false) Integer page) {
        return sourceService.findAll(pageableFactory.forSources(page));
    }

    @GetMapping("/{id}")
    public Mono<SourceResponse> getById(@PathVariable UUID id) {
        return sourceService.findById(id);
    }
}