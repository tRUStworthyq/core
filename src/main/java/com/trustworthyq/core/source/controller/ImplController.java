package com.trustworthyq.core.source.controller;

import com.trustworthyq.core.source.dto.CreateImplRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.dto.UpdateImplRequest;
import com.trustworthyq.core.source.service.ImplService;
import com.trustworthyq.core.source.utils.PageableFactory;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/impls")
@RequiredArgsConstructor
public class ImplController {

    private final ImplService implService;
    private final PageableFactory pageableFactory;

    @GetMapping
    public Mono<Page<ImplResponse>> getAll(@RequestParam(required = false) Integer page) {
        return implService.findAll(pageableFactory.forImpls(page));
    }

    @GetMapping("/{id}")
    public Mono<ImplResponse> getById(@PathVariable UUID id) {
        return implService.findById(id);
    }

    @GetMapping("/by-source/{sourceId}")
    public Mono<Page<ImplResponse>> getBySourceId(@PathVariable UUID sourceId,
                                                  @RequestParam(required = false) Integer page) {
        return implService.findBySourceId(sourceId, pageableFactory.forImpls(page));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ImplResponse> create(@Valid @RequestBody CreateImplRequest request) {
        return implService.create(request);
    }

    @PutMapping("/{id}")
    public Mono<ImplResponse> update(@PathVariable UUID id, @Valid @RequestBody UpdateImplRequest request) {
        return implService.update(id, request);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable UUID id) {
        return implService.delete(id);
    }
}