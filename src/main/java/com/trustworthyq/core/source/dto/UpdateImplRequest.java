package com.trustworthyq.core.source.dto;

import com.trustworthyq.core.source.enums.ImplState;
import com.trustworthyq.core.source.validation.ValidJson;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class UpdateImplRequest {

    @Size(min = 1, max = 255)
    @NotBlank
    private String name;

    @ValidJson
    private String config;

    private ImplState state;
}