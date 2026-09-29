package com.trustworthyq.core.chat.entity;

import java.time.Instant;
import java.util.UUID;

import com.trustworthyq.core.chat.enums.MessageRole;
import com.trustworthyq.core.chat.exception.MessageNotEditableException;
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
    private MessageRole role;
    private String content;
    private boolean edited;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant deletedAt;

    public static MessageEntity userMessage(UUID chatId, String content) {
        return create(chatId, MessageRole.USER, content);
    }

    public static MessageEntity agentMessage(UUID chatId, String content) {
        return create(chatId, MessageRole.AGENT, content);
    }

    private static MessageEntity create(UUID chatId, MessageRole messageRole, String content) {
        Instant now = Instant.now();
        MessageEntity entity = new MessageEntity();
        entity.chatId = chatId;
        entity.role = messageRole;
        entity.content = content;
        entity.edited = false;
        entity.createdAt = now;
        entity.updatedAt = now;

        return entity;
    }

    public void edit(String content) {
        if (role != MessageRole.USER) {
            throw new MessageNotEditableException(id);
        }
        this.content = content;
        this.edited = true;
        this.updatedAt = Instant.now();
    }

    public void delete() {
        this.deletedAt = Instant.now();
    }
}