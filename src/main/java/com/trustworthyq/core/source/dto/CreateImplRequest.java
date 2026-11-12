package com.trustworthyq.core.source.dto;

import com.trustworthyq.core.source.enums.ImplState;
import com.trustworthyq.core.source.validation.ValidJson;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.UUID;

@Data
public class CreateImplRequest {

    @NotBlank
    @Size(max = 255)
    private String name;

    @NotBlank
    @ValidJson
    private String config;

    private ImplState state = ImplState.NOT_RUN;

    @NotNull
    private UUID sourceId;
}