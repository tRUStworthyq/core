package com.trustworthyq.core.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class TestController {

    @GetMapping("/hello")
    public Mono<String> hello() {
        return Mono.just("hello from core");
    }

    @GetMapping("/hello/with/user")
    public Mono<String> hello(@RequestHeader(value = "X-User-Id", required = false) String userId) {
        return Mono.just("hello from core, userId=" + userId);
    }

}