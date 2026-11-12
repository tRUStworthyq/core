package com.trustworthyq.core.source.repository;

import com.trustworthyq.core.source.entity.Source;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

import org.springframework.data.domain.Pageable;
import java.util.UUID;

public interface SourceRepository extends ReactiveCrudRepository<Source, UUID> {
    Flux<Source> findAllBy(Pageable pageable);
}