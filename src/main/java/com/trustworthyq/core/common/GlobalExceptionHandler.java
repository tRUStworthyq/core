package com.trustworthyq.core.common;

import com.trustworthyq.core.chat.exception.ChatNotFoundException;
import com.trustworthyq.core.chat.exception.MessageNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebInputException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({ChatNotFoundException.class, MessageNotFoundException.class})
    public ResponseEntity<String> handleNotFound(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<String> handleValidation(WebExchangeBindException ex) {
        String message = ex.getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .findFirst()
                .orElse("Некорректные данные запроса");
        return ResponseEntity.badRequest().body(message);
    }

    @ExceptionHandler(ServerWebInputException.class)
    public ResponseEntity<String> handleBadInput(ServerWebInputException ex) {
        return ResponseEntity.badRequest().body("Некорректные параметры запроса: " + ex.getReason());
    }
}