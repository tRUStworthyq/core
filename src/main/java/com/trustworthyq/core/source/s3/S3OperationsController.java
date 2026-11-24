package com.trustworthyq.core.source.s3;

import com.trustworthyq.core.source.s3.S3Operations.ArchiveConfirmation;
import com.trustworthyq.core.source.s3.S3Operations.ArchivePreview;
import com.trustworthyq.core.source.s3.S3Operations.ArchiveResult;
import com.trustworthyq.core.source.s3.S3Graph.BucketPage;
import com.trustworthyq.core.source.s3.S3Graph.Children;
import com.trustworthyq.core.source.s3.S3Operations.ObjectLocation;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/impls/{id}/s3")
@RequiredArgsConstructor
public class S3OperationsController {
    private final S3Operations s3;
    private final S3Graph graph;
    private final S3Archive archive;

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ProblemDetail> s3Error(ResponseStatusException error) {
        return ResponseEntity.of(error.getBody()).build();
    }

    @GetMapping("/buckets")
    public Mono<BucketPage> buckets(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                    @RequestParam(defaultValue = "50") int limit,
                                    @RequestParam(required = false) String continuationToken) {
        return graph.buckets(UUID.fromString(jwt.getSubject()), id, limit, continuationToken);
    }

    @GetMapping("/buckets/{bucket}/children")
    public Mono<Children> children(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                   @PathVariable String bucket,
                                   @RequestParam(required = false) String prefix,
                                   @RequestParam(defaultValue = "50") int limit,
                                   @RequestParam(required = false) String continuationToken) {
        return graph.children(UUID.fromString(jwt.getSubject()), id, bucket, prefix, limit, continuationToken);
    }

    @PostMapping("/archive/preview")
    public Mono<ArchivePreview> preview(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                        @RequestBody ObjectLocation source) {
        return archive.preview(UUID.fromString(jwt.getSubject()), id, source);
    }

    @PostMapping("/archive/confirm")
    public Mono<ArchiveResult> confirm(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                        @RequestBody ArchiveConfirmation confirmation) {
        return archive.create(UUID.fromString(jwt.getSubject()), id, confirmation);
    }

    @GetMapping("/archive")
    public Mono<S3Archive.Page> archive(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                       @RequestParam(defaultValue = "ARCHIVED") ArchivedObject.State state,
                                       @RequestParam(defaultValue = "0") long offset,
                                       @RequestParam(defaultValue = "50") int limit) {
        return archive.page(UUID.fromString(jwt.getSubject()), id, state, offset, limit);
    }

    @PostMapping("/archive/{archiveId}/restore")
    public Mono<ArchivedObject> restore(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                        @PathVariable UUID archiveId) {
        return archive.restore(UUID.fromString(jwt.getSubject()), id, archiveId);
    }

    @PostMapping("/archive/{archiveId}/undo-restore")
    public Mono<ArchivedObject> undoRestore(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                            @PathVariable UUID archiveId) {
        return archive.undoRestore(UUID.fromString(jwt.getSubject()), id, archiveId);
    }

    @GetMapping("/archive/{archiveId}/delete-plan")
    public Mono<S3Archive.DeletePlan> deletePlan(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                                @PathVariable UUID archiveId,
                                                @RequestParam(defaultValue = "false") boolean cascade) {
        return archive.deletePlan(UUID.fromString(jwt.getSubject()), id, archiveId, cascade);
    }

    @DeleteMapping("/archive/{archiveId}")
    public Mono<ArchivedObject> deleteArchive(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                                               @PathVariable UUID archiveId,
                                               @RequestParam(defaultValue = "false") boolean cascade,
                                               @RequestBody(required = false) DeletionConfirmation confirmation) {
        return archive.delete(UUID.fromString(jwt.getSubject()), id, archiveId, cascade,
                confirmation == null ? null : confirmation.ids());
    }

    public record DeletionConfirmation(java.util.List<UUID> ids) {}
}
