package com.trustworthyq.core.source.service.impl;

import com.trustworthyq.core.source.dto.SourceResponse;
import com.trustworthyq.core.source.entity.Source;
import com.trustworthyq.core.source.repository.SourceRepository;
import com.trustworthyq.core.source.service.SourceService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.domain.Pageable;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class SourceServiceImpl implements SourceService {

    private final SourceRepository sourceRepository;

    @Override
    public Mono<Page<SourceResponse>> findAll(Pageable pageable) {
        return sourceRepository.findAllBy(pageable)
                .map(this::toDto)
                .collectList()
                .zipWith(sourceRepository.count())
                .map(t -> new PageImpl<>(t.getT1(), pageable, t.getT2()));
    }

    @Override
    public Mono<SourceResponse> findById(UUID id) {
        return sourceRepository.findById(id).map(this::toDto);
    }

    private SourceResponse toDto(Source source) {
        return SourceResponse.builder().id(source.getId()).name(source.getName()).build();
    }
}