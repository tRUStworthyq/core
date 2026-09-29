package com.trustworthyq.core.chat.entity;

import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table("messages")
public class MessageEntity {

    @Version
    private Long version;

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
        MessageEntity entity = new MessageEntity();
        entity.chatId = chatId;
        entity.content = content;
        entity.edited = false;
        entity.createdAt = now;
        entity.updatedAt = now;

        return entity;
    }

    public void edit(String content) {
        this.content = content;
        this.edited = true;
        this.updatedAt = Instant.now();
    }

    public void delete() {
        this.deletedAt = Instant.now();
    }
}