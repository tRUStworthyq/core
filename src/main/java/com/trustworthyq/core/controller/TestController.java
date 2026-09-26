package com.trustworthyq.core.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @GetMapping("/hello")
    public String hello() {
        return "hello from core";
    }

    @GetMapping("/hello/with/user")
    public String hello(@RequestHeader(value = "X-User-Id", required = false) String userId) {
        return "hello from core, userId=" + userId;
    }

}
