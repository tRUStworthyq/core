package com.trustworthyq.core.ai.service.impl;

import com.trustworthyq.core.collection.scoring.AgingCalculator.Assessment;
import com.trustworthyq.core.collection.scoring.AgingCalculator.ResponseState;
import com.trustworthyq.core.collection.service.CollectionService;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OllamaChatServiceImplTest {
    private final UUID owner = UUID.randomUUID();
    private final UUID run = UUID.randomUUID();
    private final CollectionService collections = mock(CollectionService.class);
    private final ChatClient client = mock(ChatClient.class);

    @Test
    void cleanCollectionReturnsShortAnswerWithoutCallingTheModel() {
        var keep = new Assessment("s3://test/report.txt", 0.1, 0, 6.0, "ACTIVE",
                "RECENT_READ", List.of(), null, "KEEP", ResponseState.KEEP, "NOT_DETECTED");
        when(collections.assess(owner, run, 0, 21)).thenReturn(Mono.just(List.of(keep)));

        assertThat(reply()).containsExactly("Проверенные файлы в порядке.");
        verifyNoInteractions(client);
    }

    @Test
    void emptyCollectionDoesNotReportThatItsFilesAreInOrder() {
        when(collections.assess(owner, run, 0, 21)).thenReturn(Mono.just(List.of()));

        assertThat(reply()).containsExactly("В выбранном сборе нет файлов для проверки.");
        verifyNoInteractions(client);
    }

    private List<String> reply() {
        var builder = mock(ChatClient.Builder.class);
        when(builder.build()).thenReturn(client);
        var service = new OllamaChatServiceImpl(builder, collections, 8192, 1024);
        return service.stream(List.of(), "Проверь файлы", owner, run).collectList().block(Duration.ofSeconds(2));
    }
}
