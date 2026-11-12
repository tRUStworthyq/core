package com.trustworthyq.core.source.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = JsonValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidJson {
    String message() default "Значение должно быть корректным JSON";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}