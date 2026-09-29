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
@Table("chats")
public class ChatEntity {

    @Id
    private UUID id;
    private UUID ownerId;
    private String title;
    private Instant createdAt;
    private Instant updatedAt;

    public static ChatEntity create(UUID ownerId, String title) {
        Instant now = Instant.now();
        return new ChatEntity(null, ownerId, title, now, now);
    }
}