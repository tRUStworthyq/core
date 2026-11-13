package com.trustworthyq.core.collection.service;

import com.trustworthyq.core.collection.dto.AgentRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.service.ImplService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CollectionService {

    private static final String TOPIC = "DACS_agent_request";

    private final ImplService implService;
    private final KafkaTemplate<String, AgentRequest> kafkaTemplate;

    public Mono<Void> runCollection(java.util.UUID id) {
        return implService.findById(id)
                .map(this::toAgentRequest)
                .flatMap(request ->
                        Mono.fromFuture(kafkaTemplate.send(TOPIC, request))
                )
                .then();
    }

    private AgentRequest toAgentRequest(ImplResponse impl) {
        return new AgentRequest(
                UUID.randomUUID(),
                impl.getId(),
                impl.getName(),
                impl.getConfig(),
                impl.getState(),
                impl.getSourceId()
        );
    }
}