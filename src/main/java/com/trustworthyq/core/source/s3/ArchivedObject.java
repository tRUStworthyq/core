package com.trustworthyq.core.source.s3;

import com.trustworthyq.core.source.s3.S3Operations.ObjectLocation;

import java.time.Instant;
import java.util.UUID;

public record ArchivedObject(UUID id, State state, Instant createdAt, Details details) {
    public enum State { PENDING, ARCHIVED, RESTORED, DELETED }

    public record Identity(String eTag, String versionId) {
    }

    public record Details(ObjectLocation source, ObjectLocation destination, long sizeBytes, String storageId,
                          Identity original, Identity archive, Identity restored) {
        Details restoredAs(Identity identity) {
            return new Details(source, destination, sizeBytes, storageId, original, archive, identity);
        }
    }
}
