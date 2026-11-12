package com.trustworthyq.core.source.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.pagination")
public record PaginationProperties(int sourcePageSize, int implPageSize) {
}