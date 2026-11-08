package com.trustworthyq.core.source.repository;

import com.trustworthyq.core.source.entity.Source;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;

public interface SourceRepository extends ReactiveCrudRepository<Source, UUID> {

}