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
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
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
    public Mono<Page<ImplResponse>> getAll(@AuthenticationPrincipal Jwt jwt, @RequestParam(required = false) Integer page) {
        return implService.findAll(UUID.fromString(jwt.getSubject()), pageableFactory.forImpls(page));
    }

    @GetMapping("/{id}")
    public Mono<ImplResponse> getById(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return implService.findById(UUID.fromString(jwt.getSubject()), id);
    }

    @GetMapping("/by-source/{sourceId}")
    public Mono<Page<ImplResponse>> getBySourceId(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID sourceId,
                                                  @RequestParam(required = false) Integer page) {
        return implService.findBySourceId(UUID.fromString(jwt.getSubject()), sourceId, pageableFactory.forImpls(page));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ImplResponse> create(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody CreateImplRequest request) {
        return implService.create(UUID.fromString(jwt.getSubject()), request);
    }

    @PutMapping("/{id}")
    public Mono<ImplResponse> update(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id, @Valid @RequestBody UpdateImplRequest request) {
        return implService.update(UUID.fromString(jwt.getSubject()), id, request);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return implService.delete(UUID.fromString(jwt.getSubject()), id);
    }
}