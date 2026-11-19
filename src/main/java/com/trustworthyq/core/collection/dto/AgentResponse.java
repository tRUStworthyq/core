package com.trustworthyq.core.collection.dto;

import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.UUID;

public record AgentResponse(UUID requestId, UUID id, UUID sourceId, Long batchIndex,
                            Status status, List<JsonNode> items, String error) {
    public enum Status {
        BATCH, COMPLETED, FAILED
    }

    public boolean isValid() {
        if (requestId == null || batchIndex == null || batchIndex < 0 || status == null || items == null) {
            return false;
        }
        return switch (status) {
            case BATCH -> id != null && sourceId != null && error == null && !items.isEmpty()
                    && items.stream().allMatch(item -> item != null && item.isObject());
            case COMPLETED -> id != null && sourceId != null && error == null && items.isEmpty();
            case FAILED -> error != null && !error.isBlank() && items.isEmpty();
        };
    }
}
