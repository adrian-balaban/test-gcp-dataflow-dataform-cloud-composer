package ro.mig.common;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Bridges Kafka's OAUTHBEARER SASL mechanism to a Google access token, so the mock and
 * recon can authenticate to GCP Managed Service for Apache Kafka over SASL_SSL.
 *
 * <p>Managed Kafka does not accept a username/password — it wants an OAuth bearer token
 * minted by a service account holding {@code roles/managedkafka.client}. The token is the
 * same one {@link GcpToken} already fetches for the raw-HTTP GCS/BigQuery clients: the
 * metadata-server token on Cloud Run / Composer, or {@code MIG_GCS_TOKEN} on the host.
 * Reusing {@link GcpToken#supplier()} keeps one token source for the whole run rather than
 * a second one just for Kafka.
 *
 * <p>Kafka instantiates this class reflectively from the
 * {@code sasl.login.callback.handler.class} client property, so it needs a public no-arg
 * constructor. The JAAS {@code OAuthBearerLoginModule} is still required in
 * {@code sasl.jaas.config} even with a custom handler — without it Kafka fails with
 * "No login module found for OAUTHBEARER".
 */
public final class GcpTokenOauthCallbackHandler implements AuthenticateCallbackHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final String METADATA_EMAIL_URL =
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email";

    private Supplier<String> tokenSupplier;

    public GcpTokenOauthCallbackHandler() {
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism,
                          List<AppConfigurationEntry> jaasConfigEntries) {
        this.tokenSupplier = GcpToken.supplier();
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (!(callback instanceof OAuthBearerTokenCallback cb)) {
                throw new UnsupportedCallbackException(callback,
                        "Only OAuthBearerTokenCallback is supported.");
            }
            String token = tokenSupplier == null ? null : tokenSupplier.get();
            if (token == null || token.isBlank()) {
                // No token = no authentication. Failing here surfaces the gap immediately
                // ("no broker reachable from this context") rather than letting Kafka send
                // an empty bearer and fail later with a confusing 401 on the wire.
                throw new UnsupportedCallbackException(cb,
                        "No Google access token available for OAUTHBEARER — is this running "
                                + "on GCP, or is MIG_GCS_TOKEN set?");
            }
            String principal = principal();
            if (principal == null || principal.isBlank()) {
                throw new UnsupportedCallbackException(cb,
                        "No Kafka principal available — set GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL "
                                + "to the service-account email, or run where the metadata server "
                                + "can name the default service account.");
            }
            // Managed Kafka validates the token, not the lifetime we claim; 1h is a safe
            // upper bound that matches a typical access-token TTL. The supplier refreshes
            // the underlying token itself (GcpToken caches with a 5-min margin), so this
            // lifetime only tells Kafka how long the bearer stays good.
            Instant expiry = Instant.now().plusSeconds(3600);
            cb.token(new GoogleOAuthBearerToken(kafkaToken(token, principal, expiry),
                    principal, expiry));
        }
    }

    @Override
    public void close() {
        // Nothing to release — the supplier is a plain function over env/metadata.
    }


    /**
     * The token Managed Kafka actually validates — <em>not</em> the raw access token.
     *
     * <p>Google's broker expects a three-part, dot-joined, base64url value shaped like a JWT:
     * {@code b64(header).b64(claims).b64(accessToken)}, with {@code alg=GOOG_OAUTH2_TOKEN},
     * {@code scope=kafka} and {@code sub} set to the authenticating service account's email.
     * Handing it the bare access token authenticates nothing — the broker answers
     * <em>"Authentication failed during authentication due to invalid credentials with SASL
     * mechanism OAUTHBEARER"</em>, an error that names the mechanism and not the encoding
     * (2026-08-23, recon and json_producer on the DAG). This mirrors
     * {@code com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler}, which is Google's
     * own handler for exactly this; it is reimplemented here rather than pulled in as a
     * dependency to keep {@code apps/common} free of the Google client stack it otherwise
     * does not use (the raw-HTTP GCS/BigQuery clients exist for the same reason).
     */
    static String kafkaToken(String accessToken, String principal, Instant expiry) {
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("typ", "JWT");
        header.put("alg", "GOOG_OAUTH2_TOKEN");
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("exp", expiry.getEpochSecond());
        claims.put("iat", Instant.now().getEpochSecond());
        claims.put("scope", "kafka");
        claims.put("sub", principal);
        return String.join(".", b64(json(header)), b64(json(claims)), b64(accessToken));
    }

    private static String json(Map<String, Object> value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("could not serialise the Kafka token claims", e);
        }
    }

    private static String b64(String data) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(data.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The service-account email the token claims as {@code sub}. Environment first (the
     * name Google's own handler reads, so an operator who knows one knows both), then the
     * metadata server's default service account — which is what a Composer pod under
     * Workload Identity or a Cloud Run service resolves to.
     */
    private static String principal() {
        String fromEnv = System.getenv("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(METADATA_EMAIL_URL))
                    .header("Metadata-Flavor", "Google")
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return res.statusCode() == 200 ? res.body().trim() : null;
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Minimal {@link OAuthBearerToken} carrying the Google access token and its expiry. */
    private static final class GoogleOAuthBearerToken implements OAuthBearerToken {
        private final String value;
        private final String principal;
        private final Instant expiry;

        GoogleOAuthBearerToken(String value, String principal, Instant expiry) {
            this.value = value;
            this.principal = principal;
            this.expiry = expiry;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public Long startTimeMs() {
            return null; // Unset: Kafka only uses value() + lifetimeMs() for the handshake.
        }

        @Override
        public long lifetimeMs() {
            return expiry.toEpochMilli();
        }

        @Override
        public Set<String> scope() {
            // "kafka", not "cloud-platform": this is the scope claim Google's own
            // GcpLoginCallbackHandler puts on the token it hands Kafka, and the broker
            // reads the claim rather than the scopes the access token was minted with.
            return Set.of("kafka");
        }

        @Override
        public String principalName() {
            return principal;
        }
    }
}