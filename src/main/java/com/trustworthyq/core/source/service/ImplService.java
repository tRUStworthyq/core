package com.trustworthyq.core.source.service;

import com.trustworthyq.core.source.dto.CreateImplRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.dto.UpdateImplRequest;
import org.springframework.data.domain.Page;
import reactor.core.publisher.Mono;

import org.springframework.data.domain.Pageable;
import java.util.UUID;

public interface ImplService {
    Mono<Page<ImplResponse>> findAll(Pageable pageable);
    Mono<ImplResponse> findById(UUID id);
    Mono<Page<ImplResponse>> findBySourceId(UUID sourceId, Pageable pageable);
    Mono<ImplResponse> create(CreateImplRequest request);
    Mono<ImplResponse> update(UUID id, UpdateImplRequest request);
    Mono<Void> delete(UUID id);
}