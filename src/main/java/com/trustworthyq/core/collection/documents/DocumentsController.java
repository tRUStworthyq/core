package com.trustworthyq.core.collection.documents;

import com.trustworthyq.core.collection.documents.DocumentTemplates.Template;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.core.io.Resource;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api/documents")
@RequiredArgsConstructor
public class DocumentsController {
    private final DocumentTemplates templates;
    private final DocumentIndex documents;
    private final FilesystemArchive filesystemArchive;

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ProblemDetail> error(ResponseStatusException error) {
        return ResponseEntity.of(error.getBody()).build();
    }

    @GetMapping("/templates")
    public Flux<Template> templates(@AuthenticationPrincipal Jwt jwt) {
        return templates.list(owner(jwt));
    }

    @PostMapping("/templates")
    public Mono<Template> create(@AuthenticationPrincipal Jwt jwt, @RequestBody Template value) {
        return templates.save(owner(jwt), null, value);
    }

    @PutMapping("/templates/{id}")
    public Mono<Template> update(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id, @RequestBody Template value) {
        return templates.save(owner(jwt), id, value);
    }

    @DeleteMapping("/templates/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return templates.delete(owner(jwt), id);
    }

    @GetMapping("/{impl}")
    public Mono<DocumentIndex.Page> documents(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID impl,
                                              @RequestParam(required = false) UUID run,
                                              @RequestParam(defaultValue = "") String search,
                                              @RequestParam(defaultValue = "all") String mode,
                                              @RequestParam(defaultValue = "0") long offset,
                                              @RequestParam(defaultValue = "25") int limit) {
        boolean graph = "graph".equals(mode) || "linked".equals(mode);
        return documents.load(owner(jwt), impl, run, graph).map(index -> index.page(search, mode, offset, limit));
    }

    @GetMapping("/{impl}/archive")
    public Mono<FilesystemArchive.Page> archive(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID impl,
                                                 @RequestParam(defaultValue = "0") long offset,
                                                 @RequestParam(defaultValue = "25") int limit) {
        return filesystemArchive.page(owner(jwt), impl, offset, limit);
    }

    @PostMapping("/{impl}/archive")
    public Mono<FilesystemArchive.Entry> archive(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID impl,
                                                 @RequestBody ArchiveRequest request) {
        return filesystemArchive.create(owner(jwt), impl, request.path());
    }

    @GetMapping("/{impl}/archive/{id}/download")
    public Mono<ResponseEntity<Resource>> download(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID impl,
                                                    @PathVariable UUID id) {
        return filesystemArchive.download(owner(jwt), impl, id).map(download -> {
            String path = download.entry().path();
            String name = path.substring(path.lastIndexOf('/') + 1);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.attachment()
                            .filename(name, StandardCharsets.UTF_8).build().toString())
                    .body(download.resource());
        });
    }

    public record ArchiveRequest(String path) {}

    private static UUID owner(Jwt jwt) {
        return UUID.fromString(jwt.getSubject());
    }
}
