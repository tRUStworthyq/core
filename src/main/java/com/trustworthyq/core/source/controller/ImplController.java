package com.trustworthyq.core.source.controller;

import com.trustworthyq.core.source.dto.CreateImplRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.dto.UpdateImplRequest;
import com.trustworthyq.core.source.service.ImplService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/impls")
@RequiredArgsConstructor
public class ImplController {

    private final ImplService implService;

    @GetMapping
    public Flux<ImplResponse> getAll() {
        return implService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ImplResponse> getById(@PathVariable UUID id) {
        return implService.findById(id);
    }

    @GetMapping("/by-source/{sourceId}")
    public Flux<ImplResponse> getBySourceId(@PathVariable UUID sourceId) {
        return implService.findBySourceId(sourceId);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ImplResponse> create(@Valid @RequestBody CreateImplRequest request) {
        return implService.create(request);
    }

    @PutMapping("/{id}")
    public Mono<ImplResponse> update(@PathVariable UUID id, @RequestBody UpdateImplRequest request) {
        return implService.update(id, request);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable UUID id) {
        return implService.delete(id);
    }
}