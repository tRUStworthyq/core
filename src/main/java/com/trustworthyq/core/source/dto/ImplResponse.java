package com.trustworthyq.core.source.dto;

import com.trustworthyq.core.source.enums.ImplState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImplResponse {
    private UUID id;
    private String name;
    private String config;      // JSON строкой
    private ImplState state;
    private UUID sourceId;
}