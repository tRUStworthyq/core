package com.trustworthyq.core.source.dto;

import com.trustworthyq.core.source.enums.ImplState;
import lombok.Data;

@Data
public class UpdateImplRequest {
    private String name;
    private String config;
    private ImplState state;
}