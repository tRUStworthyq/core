package com.trustworthyq.core.chat.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.pagination")
public record PaginationProperties(int chatPageSize, int messagePageSize) {
}