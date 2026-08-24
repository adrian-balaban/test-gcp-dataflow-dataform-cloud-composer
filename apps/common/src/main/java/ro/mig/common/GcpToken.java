package ro.mig.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * OAuth access tokens for the raw-HTTP GCS and BigQuery clients.
 *
 * <p>Why this exists. {@link HttpObjectStore} and {@link BigQueryRest} speak the JSON APIs
 * directly rather than through a Google client library, so they get none of Application
 * Default Credentials for free. Locally that is fine — the emulators want no auth. On
 * Composer it was not: the pods ran with a Workload Identity service account they had no
 * way to use, and the Loader died on its first real call with
 *
 * <pre>ObjectStoreException: request failed: http://localhost:4443/storage/v1/b?project=mig-local</pre>
 *
 * <p>{@code MIG_GCS_TOKEN} still wins when set — that is the seam
 * {@code local/scripts/run_pipeline.py} uses, where a token is minted on the host by
 * {@code gcloud auth}. When it is absent, and only then, this asks the GKE/GCE metadata
 * server for the pod's own token. A DAG outlives any single access token, so a token
 * cannot simply be baked into the pod spec; fetching it in-process and refreshing it is
 * the only thing that works for a long run.
 */
public final class GcpToken {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Standard GCE/GKE metadata endpoint. Reachable from any pod with Workload Identity. */
    private static final String METADATA_URL =
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

    /** Refresh early: a token that expires mid-request is the same as no token. */
    private static final Duration EXPIRY_MARGIN = Duration.ofMinutes(5);

    // One shared, thread-safe client. HttpClient is designed to be reused; having both the
    // per-request access-token fetch (get) and the once-per-run identity token (identityToken)
    // share it avoids each building a throwaway client.
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private String cached;
    private Instant expiresAt = Instant.EPOCH;

    private GcpToken() {
    }

    /**
     * A supplier that returns {@code MIG_GCS_TOKEN} when set, otherwise a metadata-server
     * token, otherwise {@code null} — which leaves the request unauthenticated, exactly the
     * behaviour the emulators expect.
     */
    public static Supplier<String> supplier() {
        String fromEnv = System.getenv("MIG_GCS_TOKEN");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return () -> fromEnv;
        }
        GcpToken instance = new GcpToken();
        return instance::get;
    }

    /**
     * An <em>identity</em> token for {@code audience}, which is what Cloud Run checks — not
     * the access token {@link #supplier()} returns. Returns {@code null} off GCP, or when
     * the audience is blank, so a loader pointed at a local mock stays unauthenticated.
     *
     * <p>Not cached: this is called once per run to build the client, whereas the access
     * token is fetched per request.
     */
    public static String identityToken(String audience) {
        if (audience == null || audience.isBlank()) {
            return null;
        }
        try {
            String url = "http://metadata.google.internal/computeMetadata/v1/instance/"
                    + "service-accounts/default/identity?audience="
                    + java.net.URLEncoder.encode(audience, java.nio.charset.StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .header("Metadata-Flavor", "Google")
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                return null;
            }
            String token = res.body().trim();
            return token.isEmpty() ? null : token;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    private synchronized String get() {
        if (cached != null && Instant.now().isBefore(expiresAt)) {
            return cached;
        }
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(METADATA_URL))
                    // The metadata server refuses any request without this header, which is
                    // what stops a confused browser or SSRF from harvesting the token.
                    .header("Metadata-Flavor", "Google")
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                return null;
            }
            JsonNode body = MAPPER.readTree(res.body());
            String token = body.path("access_token").asText(null);
            long expiresIn = body.path("expires_in").asLong(0);
            if (token == null || token.isBlank()) {
                return null;
            }
            cached = token;
            expiresAt = Instant.now().plusSeconds(expiresIn).minus(EXPIRY_MARGIN);
            return cached;
        } catch (Exception e) {
            // Off GCP there is no metadata server, and that is not an error: the caller is
            // talking to an emulator. Returning null sends the request unauthenticated,
            // and a genuinely unauthorized call fails later with a 401 that names itself.
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}
