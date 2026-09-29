package com.trustworthyq.core.chat.entity;

import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table("messages")
public class MessageEntity {

    @Id
    private UUID id;
    private UUID chatId;
    private String content;
    private boolean edited;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant deletedAt;

    public static MessageEntity create(UUID chatId, String content) {
        Instant now = Instant.now();
        return new MessageEntity(null, chatId, content, false, now, now, null);
    }
}