package com.trustworthyq.core.source.service;

import com.trustworthyq.core.source.dto.CreateImplRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.dto.UpdateImplRequest;
import com.trustworthyq.core.source.entity.Impl;
import com.trustworthyq.core.source.enums.ImplState;
import com.trustworthyq.core.source.repository.ImplRepository;
import com.trustworthyq.core.source.repository.SourceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ImplService {

    private final ImplRepository implRepository;
    private final SourceRepository sourceRepository;

    public Flux<ImplResponse> findAll() {
        return implRepository.findAll().map(this::toDto);
    }

    public Mono<ImplResponse> findById(UUID id) {
        return implRepository.findById(id)
                .map(this::toDto)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Impl not found")));
    }

    public Flux<ImplResponse> findBySourceId(UUID sourceId) {
        return implRepository.findBySourceId(sourceId).map(this::toDto);
    }

    public Mono<ImplResponse> create(CreateImplRequest request) {
        return sourceRepository.findById(request.getSourceId())
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Source not found")))
                .flatMap(source -> {
                    Impl impl = Impl.builder()
                            .name(request.getName())
                            .config(request.getConfig())
                            .state(request.getState() != null ? request.getState() : ImplState.NOT_RUN)
                            .sourceId(request.getSourceId())
                            .build();
                    return implRepository.save(impl);
                })
                .map(this::toDto);
    }

    public Mono<ImplResponse> update(UUID id, UpdateImplRequest request) {
        return implRepository.findById(id)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Impl not found")))
                .flatMap(existing -> {
                    if (request.getName() != null) {
                        existing.setName(request.getName());
                    }
                    if (request.getConfig() != null) {
                        existing.setConfig(request.getConfig());
                    }
                    if (request.getState() != null) {
                        existing.setState(request.getState());
                    }
                    return implRepository.save(existing);
                })
                .map(this::toDto);
    }

    public Mono<Void> delete(UUID id) {
        return implRepository.findById(id)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Impl not found")))
                .flatMap(implRepository::delete);
    }

    private ImplResponse toDto(Impl impl) {
        return ImplResponse.builder()
                .id(impl.getId())
                .name(impl.getName())
                .config(impl.getConfig())
                .state(impl.getState())
                .sourceId(impl.getSourceId())
                .build();
    }
}