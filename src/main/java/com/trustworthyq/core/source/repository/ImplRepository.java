package com.trustworthyq.core.source.repository;

import com.trustworthyq.core.source.entity.Impl;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface ImplRepository extends ReactiveCrudRepository<Impl, UUID> {

    Flux<Impl> findBySourceId(UUID sourceId);
}