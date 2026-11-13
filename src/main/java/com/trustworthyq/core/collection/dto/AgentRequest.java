package com.trustworthyq.core.collection.dto;

import com.trustworthyq.core.source.enums.ImplState;

import java.util.UUID;

public record AgentRequest(
        UUID requestId,
        UUID id,
        String name,
        String config,
        ImplState state,
        UUID sourceId
) {
}