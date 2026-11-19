package com.trustworthyq.core.collection.service;

import com.trustworthyq.core.collection.dto.AgentRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.service.ImplService;
import com.trustworthyq.core.utils.UUIDUtils;
import com.trustworthyq.core.collection.repository.AgentResponseRepository;
import com.trustworthyq.core.collection.repository.AgentResponseRepository.RunStatus;
import com.trustworthyq.core.collection.scoring.AgingCalculator;
import com.trustworthyq.core.collection.scoring.AgingCalculator.Assessment;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CollectionService {

    @Value("${app.kafka.topic}")
    private String topic;

    private final ImplService implService;
    private final KafkaTemplate<String, AgentRequest> kafkaTemplate;

    private static final UUID S3 = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14");
    private final AgentResponseRepository responses;
    private final JsonMapper mapper;
    private final AgingCalculator aging;

    public Mono<RunStatus> runCollection(UUID ownerId, UUID id) {
        return implService.findById(ownerId, id)
                .filter(impl -> S3.equals(impl.getSourceId()))
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only S3 is supported")))
                .map(this::toAgentRequest)
                .flatMap(request -> responses.start(request.requestId(), ownerId, id)
                        .then(Mono.defer(() -> Mono.fromFuture(kafkaTemplate.send(topic, request))))
                        .onErrorResume(error -> responses.dispatchFailed(request.requestId()).then(Mono.error(error)))
                        .thenReturn(new RunStatus(request.requestId(), "QUEUED", 0, null)));
    }

    public Mono<RunStatus> status(UUID ownerId, UUID requestId) {
        return responses.status(requestId, ownerId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Collection run not found")));
    }

    public Mono<List<JsonNode>> objects(UUID ownerId, UUID requestId, long offset, int limit, String piiStatus) {
        return status(ownerId, requestId).thenMany(responses.objects(requestId, ownerId, Math.max(0, offset),
                Math.clamp(limit, 1, 200), piiStatus == null ? "" : piiStatus, mapper)).collectList();
    }
    public Mono<RunStatus> completed(UUID ownerId, UUID requestId) {
        return status(ownerId, requestId).filter(run -> "COMPLETED".equals(run.status()))
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.CONFLICT, "Collection is not complete")));
    }

    public Mono<List<Assessment>> assess(UUID ownerId, UUID requestId, long offset, int limit) {
        return completed(ownerId, requestId)
                .thenMany(responses.scoringObjects(requestId, ownerId, Math.max(0, offset),
                        Math.clamp(limit, 1, 200), mapper))
                .map(aging::assess).collectList();
    }

    private AgentRequest toAgentRequest(ImplResponse impl) {
        return new AgentRequest(
                UUIDUtils.generateRandomUUID(),
                impl.getId(),
                impl.getName(),
                impl.getConfig(),
                impl.getState(),
                impl.getSourceId()
        );
    }
}