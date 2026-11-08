package com.trustworthyq.core.source.service;

import com.trustworthyq.core.source.dto.SourceResponse;
import com.trustworthyq.core.source.entity.Source;
import com.trustworthyq.core.source.repository.SourceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class SourceService {

    private final SourceRepository sourceRepository;

    public Flux<SourceResponse> findAll() {
        return sourceRepository.findAll()
                .map(this::toDto);
    }

    public Mono<SourceResponse> findById(UUID id) {
        return sourceRepository.findById(id)
                .map(this::toDto);
    }

    private SourceResponse toDto(Source source) {
        return SourceResponse.builder()
                .id(source.getId())
                .name(source.getName())
                .build();
    }
}