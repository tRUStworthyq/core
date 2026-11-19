package com.trustworthyq.core;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class CoreApplicationTests {

    @Test
    void contextLoads() {
    }

}
