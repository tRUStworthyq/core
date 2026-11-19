package com.trustworthyq.core.collection.listener;

import com.trustworthyq.core.collection.dto.AgentResponse;
import com.trustworthyq.core.collection.exception.InvalidAgentResponseException;
import com.trustworthyq.core.collection.repository.AgentResponseRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

@Component
public class AgentResponseListener {
    private final JsonMapper json;
    private final AgentResponseRepository repository;

    public AgentResponseListener(JsonMapper json, AgentResponseRepository repository) {
        this.json = json;
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.kafka.response-topic}")
    public void onResponse(String payload) {
        AgentResponse response;
        try {
            response = json.readValue(payload, AgentResponse.class);
        } catch (JacksonException exception) {
            throw new InvalidAgentResponseException();
        }
        if (response == null || !response.isValid()) {
            throw new InvalidAgentResponseException();
        }

        // Kafka advances the offset after the PostgreSQL write succeeds.
        repository.save(payload).block(Duration.ofSeconds(30));
    }
}
