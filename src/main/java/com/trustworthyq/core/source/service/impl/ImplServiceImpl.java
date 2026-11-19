package com.trustworthyq.core.source.service.impl;

import com.trustworthyq.core.source.dto.CreateImplRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.dto.UpdateImplRequest;
import com.trustworthyq.core.source.entity.Impl;
import com.trustworthyq.core.source.enums.ImplState;
import com.trustworthyq.core.source.repository.ImplRepository;
import com.trustworthyq.core.source.repository.SourceRepository;
import com.trustworthyq.core.source.service.ImplService;
import com.trustworthyq.core.utils.UUIDUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import org.springframework.data.domain.Pageable;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ImplServiceImpl implements ImplService {

    private final ImplRepository implRepository;
    private final SourceRepository sourceRepository;

    @Override
    public Mono<Page<ImplResponse>> findAll(UUID ownerId, Pageable pageable) {
        return implRepository.findByOwnerId(ownerId, pageable)
                .map(this::toDto)
                .collectList()
                .zipWith(implRepository.countByOwnerId(ownerId))
                .map(t -> new PageImpl<>(t.getT1(), pageable, t.getT2()));
    }

    @Override
    public Mono<ImplResponse> findById(UUID ownerId, UUID id) {
        return implRepository.findByIdAndOwnerId(id, ownerId)
                .map(this::toDto)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Impl not found")));
    }

    @Override
    public Mono<Page<ImplResponse>> findBySourceId(UUID ownerId, UUID sourceId, Pageable pageable) {
        return implRepository.findBySourceIdAndOwnerId(sourceId, ownerId, pageable)
                .map(this::toDto)
                .collectList()
                .zipWith(implRepository.countBySourceIdAndOwnerId(sourceId, ownerId))
                .map(t -> new PageImpl<>(t.getT1(), pageable, t.getT2()));
    }

    @Override
    public Mono<ImplResponse> create(UUID ownerId, CreateImplRequest request) {
        return sourceRepository.findById(request.getSourceId())
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Source not found")))
                .flatMap(source -> {
                    Impl impl = Impl.builder()
                            .id(UUIDUtils.generateRandomUUID())
                            .ownerId(ownerId)
                            .name(request.getName())
                            .config(request.getConfig())
                            .state(request.getState() != null ? request.getState() : ImplState.NOT_RUN)
                            .sourceId(request.getSourceId())
                            .isNew(true)
                            .build();
                    return implRepository.saveWithJsonbCast(impl);
                })
                .map(this::toDto);
    }

    @Override
    public Mono<ImplResponse> update(UUID ownerId, UUID id, UpdateImplRequest request) {
        return implRepository.findByIdAndOwnerId(id, ownerId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Impl not found")))
                .map(existing -> applyUpdate(existing, request))
                .flatMap(implRepository::updateWithJsonbCast)
                .map(this::toDto);
    }

    @Override
    public Mono<Void> delete(UUID ownerId, UUID id) {
        return implRepository.findByIdAndOwnerId(id, ownerId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Impl not found")))
                .flatMap(implRepository::delete);
    }

    private Impl applyUpdate(Impl existing, UpdateImplRequest request) {
        existing.setNew(false);
        if (request.getName() != null) {
            existing.setName(request.getName());
        }
        if (request.getConfig() != null) {
            existing.setConfig(request.getConfig());
        }
        if (request.getState() != null) {
            existing.setState(request.getState());
        }
        return existing;
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