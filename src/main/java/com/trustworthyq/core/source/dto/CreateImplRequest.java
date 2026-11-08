package com.trustworthyq.core.source.dto;

import com.trustworthyq.core.source.enums.ImplState;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.UUID;

@Data
public class CreateImplRequest {

    @NotBlank
    private String name;

    @NotBlank
    private String config;

    private ImplState state = ImplState.NOT_RUN;

    @NotNull
    private UUID sourceId;
}