package com.trustworthyq.core.source.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class JsonValidator implements ConstraintValidator<ValidJson, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        try {
            MAPPER.readTree(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}