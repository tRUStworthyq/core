package com.trustworthyq.core.source.s3;

import com.trustworthyq.core.source.service.ImplService;
import com.trustworthyq.core.source.s3.ArchivedObject.Details;
import com.trustworthyq.core.source.s3.ArchivedObject.Identity;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

@Service
@RequiredArgsConstructor
public class S3Operations {
    private static final UUID S3 = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14");
    private static final long COPY_LIMIT = 5L * 1024 * 1024 * 1024;

    private final ImplService implementations;
    private final JsonMapper json;

    public Mono<ArchivePreview> archivePreview(UUID owner, UUID id, ObjectLocation source) {
        return execute(owner, id, (client, config) -> preview(client, config, id, source));
    }

    public Mono<ArchiveResult> archive(UUID owner, UUID id, ArchiveConfirmation confirmation) {
        if (!confirmation.confirmed() || confirmation.preview() == null) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit archive confirmation required"));
        }
        return execute(owner, id, (client, config) -> {
            ArchivePreview accepted = confirmation.preview();
            ArchivePreview current = preview(client, config, id, accepted.source());
            if (!current.equals(accepted)) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Archive preview changed. Confirm the new preview.");
            }
            Identity copied;
            try {
                var copy = client.copyObject(request -> request.sourceBucket(current.source().bucket())
                        .sourceKey(current.source().key()).sourceVersionId(current.versionId())
                        .copySourceIfMatch(current.eTag())
                        .overrideConfiguration(options -> options.putHeader("If-None-Match", "*"))
                        .destinationBucket(current.destination().bucket()).destinationKey(current.destination().key()));
                copied = new Identity(copy.copyObjectResult().eTag(), copy.versionId());
            } catch (S3Exception exception) {
                if (exception.statusCode() != 412) {
                    throw exception;
                }
                var previous = head(client, current.destination(), null);
                if (previous == null || previous.contentLength() != current.sizeBytes()
                        || !Objects.equals(previous.eTag(), current.eTag())) {
                    throw exception;
                }
                copied = identity(previous);
            }
            var details = new Details(current.source(), current.destination(), current.sizeBytes(),
                    storageId(config), new Identity(current.eTag(), current.versionId()), copied, null);
            return new ArchiveResult("ARCHIVED", current.source(), current.destination(), copied.versionId(), false,
                    archiveId(current.destination()), details);
        });
    }

    Mono<Identity> restore(UUID owner, UUID id, Details details) {
        return execute(owner, id, (client, config) -> {
            checkStorage(config, details);
            var existing = head(client, details.source(), null);
            if (existing != null) {
                Identity identity = identity(existing);
                if (identity.equals(details.original()) || identity.equals(details.restored())) {
                    return identity;
                }
                throw new ResponseStatusException(HttpStatus.CONFLICT, "A different object occupies the original key");
            }
            requireArchive(client, details);
            var result = client.copyObject(request -> request
                    .sourceBucket(details.destination().bucket()).sourceKey(details.destination().key())
                    .sourceVersionId(details.archive().versionId()).copySourceIfMatch(details.archive().eTag())
                    .overrideConfiguration(options -> options.putHeader("If-None-Match", "*"))
                    .destinationBucket(details.source().bucket()).destinationKey(details.source().key()));
            return new Identity(result.copyObjectResult().eTag(), result.versionId());
        });
    }

    Mono<Void> verifyUndo(UUID owner, UUID id, Details details) {
        return execute(owner, id, (client, config) -> {
            checkStorage(config, details);
            var live = head(client, details.source(), null);
            if (live == null || !identity(live).equals(details.restored())) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Restored object is missing or changed at the original key");
            }
            requireArchive(client, details);
            return true;
        }).then();
    }

    Mono<Void> verifyDeletion(UUID owner, UUID id, Details details, Instant modified) {
        return deleteArchive(owner, id, details, modified, true);
    }

    Mono<Void> deleteArchive(UUID owner, UUID id, Details details, Instant modified) {
        return deleteArchive(owner, id, details, modified, false);
    }

    private Mono<Void> deleteArchive(UUID owner, UUID id, Details details, Instant modified, boolean dryRun) {
        return execute(owner, id, (client, config) -> {
            checkStorage(config, details);
            var source = head(client, details.source(), null);
            var expectedSource = details.restored() == null ? details.original() : details.restored();
            if (source != null && (!identity(source).equals(expectedSource) || !Objects.equals(modified, source.lastModified()))) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Original object has changed");
            }
            var copy = head(client, details.destination(), details.archive().versionId());
            if (copy != null && !identity(copy).equals(details.archive())) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Archive copy has changed");
            }
            if (dryRun) return true;
            if (source != null) {
                client.deleteObject(request -> request.bucket(details.source().bucket())
                        .key(details.source().key())
                        .overrideConfiguration(options -> options.putHeader("If-Match", expectedSource.eTag())));
            }
            if (copy != null) {
                client.deleteObject(request -> request.bucket(details.destination().bucket())
                        .key(details.destination().key()).versionId(details.archive().versionId())
                        .overrideConfiguration(options -> options.putHeader("If-Match", details.archive().eTag())));
            }
            return true;
        }).then();
    }

    private static HeadObjectResponse head(S3Client client, ObjectLocation location, String versionId) {
        try {
            return client.headObject(request -> request.bucket(location.bucket()).key(location.key()).versionId(versionId));
        } catch (S3Exception exception) {
            if (exception.statusCode() == 404) {
                return null;
            }
            throw exception;
        }
    }

    private static Identity identity(HeadObjectResponse object) {
        return new Identity(object.eTag(), object.versionId());
    }

    private static void requireArchive(S3Client client, Details details) {
        var copy = head(client, details.destination(), details.archive().versionId());
        if (copy == null || !identity(copy).equals(details.archive())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Archive copy is missing or changed");
        }
    }

    private static String storageId(Config config) {
        return sha256(config.endpoint().toString());
    }

    private static void checkStorage(Config config, Details details) {
        if (!storageId(config).equals(details.storageId())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "S3 endpoint changed since archiving");
        }
    }

    private ArchivePreview preview(S3Client client, Config config, UUID id, ObjectLocation source) {
        if (source == null || source.bucket() == null || source.bucket().isBlank()
                || source.key() == null || source.key().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Bucket and object key are required");
        }
        String archiveBucket = config.archiveBucket();
        if (archiveBucket == null || archiveBucket.isBlank() || archiveBucket.equals(source.bucket())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Configure a separate archiveBucket in the Impl");
        }
        if (source.bucket().equals(config.bucket()) && !source.key().startsWith(config.prefix())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Object is outside the configured source prefix");
        }
        var head = client.headObject(request -> request.bucket(source.bucket()).key(source.key()));
        if (head.contentLength() > COPY_LIMIT || head.eTag() == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_CONTENT, "Object cannot be archived by single-copy operation");
        }
        String identity = json.writeValueAsString(List.of(config.endpoint().toString(),
                source.bucket(), source.key(), head.eTag(), Objects.toString(head.versionId(), ""),
                Objects.toString(head.lastModified(), "")));
        String key = "dacs-archive/managed/" + id + "/" + sha256(identity);
        return new ArchivePreview("ARCHIVE_CONFIRMATION_REQUIRED", source, new ObjectLocation(archiveBucket, key),
                head.eTag(), head.versionId(), head.contentLength(), false, head.lastModified());
    }

    private <T> Mono<T> execute(UUID owner, UUID id, BiFunction<S3Client, Config, T> operation) {
        return implementations.findById(owner, id)
                .filter(impl -> S3.equals(impl.getSourceId()))
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only S3 is supported")))
                .flatMap(impl -> Mono.fromCallable(() -> {
                    Config config = readConfig(impl.getConfig());
                    try (S3Client client = client(config)) {
                        return operation.apply(client, config);
                    } catch (S3Exception exception) {
                        var details = exception.awsErrorDetails();
                        if (details != null && "NoSuchBucket".equals(details.errorCode())) {
                            throw new ResponseStatusException(HttpStatus.CONFLICT,
                                    "Бакет S3 не найден. Проверьте bucket и archiveBucket в настройках подключения.");
                        }
                        HttpStatus status = switch (exception.statusCode()) {
                            case 400 -> HttpStatus.BAD_REQUEST;
                            case 401, 403 -> HttpStatus.FORBIDDEN;
                            case 404 -> HttpStatus.NOT_FOUND;
                            case 409, 412 -> HttpStatus.CONFLICT;
                            default -> HttpStatus.BAD_GATEWAY;
                        };
                        throw new ResponseStatusException(status, "S3 operation rejected");
                    } catch (SdkClientException exception) {
                        throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "S3 endpoint unavailable");
                    }
                }).subscribeOn(Schedulers.boundedElastic()));
    }

    private Config readConfig(String text) {
        try {
            JsonNode node = json.readTree(text);
            URI endpoint = URI.create(node.path("endpoint").asString(""));
            String accessKey = node.path("accessKey").asString("");
            String secretKey = node.path("secretKey").asString("");
            if (endpoint.getHost() == null || endpoint.getUserInfo() != null
                    || endpoint.getQuery() != null || endpoint.getFragment() != null
                    || !("http".equals(endpoint.getScheme()) || "https".equals(endpoint.getScheme()))
                    || accessKey.isBlank() || secretKey.isBlank()) {
                throw new IllegalArgumentException();
            }
            return new Config(endpoint, node.path("region").asString("us-east-1"),
                    node.path("bucket").asString(""), node.path("prefix").asString(""), accessKey, secretKey,
                    node.path("sessionToken").asString(""), node.path("archiveBucket").asString(""));
        } catch (RuntimeException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid S3 Impl configuration");
        }
    }

    private static S3Client client(Config config) {
        var credentials = config.sessionToken().isBlank()
                ? AwsBasicCredentials.create(config.accessKey(), config.secretKey())
                : AwsSessionCredentials.create(config.accessKey(), config.secretKey(), config.sessionToken());
        return S3Client.builder().endpointOverride(config.endpoint()).forcePathStyle(true)
                .region(Region.of(config.region().isBlank() ? "us-east-1" : config.region()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .overrideConfiguration(options -> options.apiCallTimeout(Duration.ofSeconds(45))
                        .apiCallAttemptTimeout(Duration.ofSeconds(30)))
                .build();
    }

    static UUID archiveId(ObjectLocation destination) {
        return UUID.nameUUIDFromBytes((destination.bucket() + "/" + destination.key())
                .getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private record Config(URI endpoint, String region, String bucket, String prefix, String accessKey,
                          String secretKey, String sessionToken, String archiveBucket) {
    }

    public record ObjectLocation(String bucket, String key) {
    }

    public record ArchivePreview(String state, ObjectLocation source, ObjectLocation destination, String eTag, String versionId,
                                 long sizeBytes, boolean sourceDeleted, Instant modifiedAt) {
    }

    public record ArchiveConfirmation(boolean confirmed, ArchivePreview preview) {
    }

    public record ArchiveResult(String state, ObjectLocation source, ObjectLocation destination,
                                String archiveVersionId, boolean sourceDeleted, UUID archiveId,
                                @com.fasterxml.jackson.annotation.JsonIgnore Details details) {
    }
}
