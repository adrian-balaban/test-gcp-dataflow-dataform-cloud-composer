package ro.mig.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@link ObjectStore} over the GCS JSON API, using only the JDK HTTP client.
 *
 * <p>Points at fake-gcs-server locally. The same code reaches real GCS by setting the host to
 * {@code https://storage.googleapis.com} and supplying a bearer token; production would swap in
 * the google-cloud-storage client for retries, resumable uploads and CMEK support — see
 * docs/runbook-gcp.md.
 */
public final class HttpObjectStore implements ObjectStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String host;
    private final HttpClient http;
    private final Supplier<String> bearerToken;

    public HttpObjectStore(String host, Supplier<String> bearerToken) {
        this.host = host.replaceAll("/+$", "");
        this.bearerToken = bearerToken;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /** Local default: fake-gcs-server needs no credentials. */
    /** True when pointed at Google's own endpoint rather than an emulator. */
    private boolean isRealGcs() {
        return host.contains("storage.googleapis.com");
    }

    public static HttpObjectStore local(String host) {
        return new HttpObjectStore(host, () -> null);
    }

    /**
     * Environment-driven: {@code MIG_GCS_TOKEN} (an OAuth access token) is attached as the
     * bearer when set, which is what makes the same code reach real GCS. The orchestrator
     * (local/scripts/run_pipeline.py) exports it on the real profile.
     *
     * <p>When it is not set — every pod the Composer DAG launches — {@link GcpToken} falls
     * back to the metadata server, so a pod authenticates as its own Workload Identity
     * account. Off GCP both sources come up empty and this behaves exactly like
     * {@link #local}, which is what the emulators want.
     */
    public static HttpObjectStore fromEnv(String host) {
        return new HttpObjectStore(host, GcpToken.supplier());
    }

    private HttpRequest.Builder request(String url) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(60));
        String token = bearerToken.get();
        if (token != null && !token.isBlank()) {
            b.header("Authorization", "Bearer " + token);
        }
        return b;
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private <T> HttpResponse<T> send(HttpRequest req, HttpResponse.BodyHandler<T> handler) {
        try {
            return http.send(req, handler);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ObjectStoreException("request failed: " + req.uri(), e);
        }
    }

    @Override
    public void createBucket(String bucket) {
        // On real GCS the buckets are Terraform-owned and already exist, and the pods run
        // as the least-privilege dataflow-worker service account, which deliberately has
        // no project-level storage.buckets.create. Attempting to create anyway turned a
        // correctly-scoped IAM policy into a task failure:
        //
        //   403 dataflow-worker@… does not have storage.buckets.create access …
        //
        // It only surfaced under Composer: a laptop run authenticates as a human with far
        // broader rights, so the call quietly succeeded there. Auto-creation exists for
        // fake-gcs-server, which starts empty, so it is scoped to the emulator.
        if (isRealGcs()) {
            return;
        }

        String body = "{\"name\":\"" + bucket + "\"}";
        String project = System.getenv().getOrDefault("MIG_GCS_PROJECT", "mig-local");
        HttpRequest req = request(host + "/storage/v1/b?project=" + enc(project))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> res = send(req, HttpResponse.BodyHandlers.ofString());
        // 409 = already exists, which is the normal case on a re-run.
        if (res.statusCode() != 200 && res.statusCode() != 409) {
            throw new ObjectStoreException(
                    "createBucket " + bucket + " -> HTTP " + res.statusCode() + ": " + res.body(), null);
        }
    }

    @Override
    public void put(String bucket, String objectName, byte[] content) {
        String url = host + "/upload/storage/v1/b/" + enc(bucket)
                + "/o?uploadType=media&name=" + enc(objectName);
        HttpRequest req = request(url)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(content))
                .build();
        HttpResponse<String> res = send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) {
            throw new ObjectStoreException(
                    "put gs://" + bucket + "/" + objectName + " -> HTTP " + res.statusCode()
                            + ": " + res.body(), null);
        }
    }

    @Override
    public byte[] get(String bucket, String objectName) {
        String url = host + "/storage/v1/b/" + enc(bucket) + "/o/" + enc(objectName) + "?alt=media";
        HttpResponse<byte[]> res = send(request(url).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        if (res.statusCode() / 100 != 2) {
            throw new ObjectStoreException(
                    "get gs://" + bucket + "/" + objectName + " -> HTTP " + res.statusCode(), null);
        }
        return res.body();
    }

    /**
     * List every object under a prefix, following {@code nextPageToken} to the end.
     *
     * <p>GCS caps a listing at 1000 objects per page. Reading only the first page
     * silently truncated the result, and the loader would then write a {@code .FLG}
     * vouching for a complete run over a partial file set — a wrong answer that looks
     * like a right one. Pagination is therefore correctness, not throughput.
     */
    @Override
    public List<String> list(String bucket, String prefix) {
        List<String> names = new ArrayList<>();
        String pageToken = null;
        do {
            String url = host + "/storage/v1/b/" + enc(bucket) + "/o?prefix=" + enc(prefix);
            if (pageToken != null) {
                url += "&pageToken=" + enc(pageToken);
            }
            HttpResponse<String> res =
                    send(request(url).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() / 100 != 2) {
                throw new ObjectStoreException(
                        "list gs://" + bucket + "/" + prefix + " -> HTTP " + res.statusCode(), null);
            }
            try {
                JsonNode root = MAPPER.readTree(res.body());
                for (JsonNode item : root.path("items")) {
                    names.add(item.path("name").asText());
                }
                JsonNode next = root.path("nextPageToken");
                pageToken = next.isMissingNode() || next.isNull() || next.asText().isBlank()
                        ? null
                        : next.asText();
            } catch (IOException e) {
                throw new ObjectStoreException("could not parse listing for " + bucket, e);
            }
        } while (pageToken != null);
        names.sort(String::compareTo);
        return names;
    }

    /**
     * Whether an object exists.
     *
     * <p>Only 404 means "absent". A 403 means the caller cannot see the object and a 5xx
     * means the answer is unknown — reporting either as "does not exist" turns a
     * permissions or outage problem into a wrong business conclusion (for example, a
     * missing {@code .FLG} semaphore read as "the extract is not ready yet", forever).
     */
    @Override
    public boolean exists(String bucket, String objectName) {
        String url = host + "/storage/v1/b/" + enc(bucket) + "/o/" + enc(objectName);
        int status = send(request(url).GET().build(), HttpResponse.BodyHandlers.discarding())
                .statusCode();
        if (status / 100 == 2) {
            return true;
        }
        if (status == 404) {
            return false;
        }
        throw new ObjectStoreException(
                "exists gs://" + bucket + "/" + objectName + " -> HTTP " + status
                        + " (not a definitive answer; refusing to report the object as absent)",
                null);
    }

    /** Unchecked so callers are not forced to wrap every storage call. */
    public static final class ObjectStoreException extends RuntimeException {
        public ObjectStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
