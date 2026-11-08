package com.trustworthyq.core.source.repository;

import com.trustworthyq.core.source.entity.Source;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface SourceRepository extends ReactiveCrudRepository<Source, UUID> {

    Mono<Source> findByName(String name);
}