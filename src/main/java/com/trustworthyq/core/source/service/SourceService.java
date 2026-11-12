package com.trustworthyq.core.source.service;

import com.trustworthyq.core.source.dto.SourceResponse;
import org.springframework.data.domain.Page;
import reactor.core.publisher.Mono;

import org.springframework.data.domain.Pageable;
import java.util.UUID;

public interface SourceService {
    Mono<Page<SourceResponse>> findAll(Pageable pageable);
    Mono<SourceResponse> findById(UUID id);
}