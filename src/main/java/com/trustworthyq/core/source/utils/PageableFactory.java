package com.trustworthyq.core.source.utils;

import com.trustworthyq.core.source.config.PaginationProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PageableFactory {

    private final PaginationProperties props;

    public Pageable forSources(Integer page) {
        return PageRequest.of(page == null ? 0 : Math.max(page, 0), props.sourcePageSize());
    }

    public Pageable forImpls(Integer page) {
        return PageRequest.of(page == null ? 0 : Math.max(page, 0), props.implPageSize());
    }
}