package com.trustworthyq.core.source.entity;

import com.trustworthyq.core.source.enums.ImplState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("impl")
public class Impl implements Persistable<UUID> {

    @Id
    private UUID id;

    private String name;

    private String config;

    private ImplState state;

    @Column("source_id")
    private UUID sourceId;

    @Transient
    @Builder.Default
    private boolean isNew = true;

    @Override
    public boolean isNew() {
        return isNew;
    }
}