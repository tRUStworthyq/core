package com.trustworthyq.core.collection.service;

import com.trustworthyq.core.collection.dto.AgentRequest;
import com.trustworthyq.core.source.dto.ImplResponse;
import com.trustworthyq.core.source.service.ImplService;
import com.trustworthyq.core.utils.UUIDUtils;
import lombok.RequiredArgsConstructor;
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

    public Mono<Void> runCollection(UUID id) {
        return implService.findById(id)
                .map(this::toAgentRequest)
                .flatMap(request ->
                        Mono.fromFuture(kafkaTemplate.send(topic, request))
                )
                .then();
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