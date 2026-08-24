package ro.mig.vault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stand-in for Target System's loader APIs.
 *
 * <p>Exists so the Load lane can be exercised end-to-end before the real contract is available
 * (we own neither the Extractor nor the Loader). It deliberately misbehaves: a configurable
 * share of requests return 429 or 503 so the Loader App's retry and backoff paths are actually
 * executed rather than merely written.
 *
 * <p>Idempotency is keyed on the {@code X-Idempotency-Key} header, which the Loader populates
 * with the engine's deterministic dedup key. That is what makes at-least-once delivery safe:
 * a replayed batch produces no duplicate accounts here.
 *
 * <pre>
 *   POST /v1/accounts                       load one account document
 *   GET  /v1/accounts/{id}                  read one back
 *   GET  /__admin/stats                     counters, for reconciliation
 *   POST /__admin/reset                     clear state between runs
 *   POST /__admin/suppress-next-confirmation   one-shot: next 201 stores but does not publish
 * </pre>
 *
 * <p>When {@code TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP} is set, every accepted write (201)
 * additionally publishes a confirmation event to {@code TARGET_SYSTEM_CONFIRMATION_TOPIC}
 * — JSON {@code {runId, accountId, accountKey, confirmedAt}} keyed by {@code accountKey}
 * — so recon can prove the row was actually persisted rather than merely posted (see
 * docs/PLAN-CHANGES-22082026.md). A replayed batch returns 200 and publishes nothing, so
 * the confirmation stream has the same idempotency semantics as the write path. An empty
 * bootstrap means no producer is built and the mock behaves exactly as before, which is
 * what keeps a no-Kafka run green.
 */
public final class TargetSystemMock {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<String, JsonNode> accounts = new ConcurrentHashMap<>();
    private final Map<String, String> idempotency = new ConcurrentHashMap<>();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong injectedFailures = new AtomicLong();
    private final AtomicLong confirmationsPublished = new AtomicLong();
    // RC9 hook: a one-shot flag set by /__admin/suppress-next-confirmation. The next
    // accepted write still returns 201 and stores the account, but skips the publish —
    // manufacturing "sent but not confirmed" deterministically, never in a real path.
    private final AtomicBoolean suppressNextConfirmation = new AtomicBoolean(false);

    private final double failureRate;
    private final Random random;
    // Null when no bootstrap is configured → every confirmation path is a no-op.
    private final KafkaProducer<String, String> producer;
    private final String confirmationTopic;

    public TargetSystemMock(double failureRate, long seed) {
        this(failureRate, seed, null, null);
    }

    public TargetSystemMock(double failureRate, long seed,
                            String confirmationBootstrap, String confirmationTopic) {
        this.failureRate = failureRate;
        this.random = new Random(seed);
        this.confirmationTopic = confirmationTopic;
        this.producer = confirmationBootstrap == null || confirmationBootstrap.isBlank()
                ? null
                : buildProducer(confirmationBootstrap);
    }

    private static KafkaProducer<String, String> buildProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // The confirmation stream is tiny and per-run; keep the sends synchronous so a
        // broker failure is visible on the call that caused it rather than in a callback
        // the handler has already returned past. idempotence off is fine — a replayed
        // batch returns 200 and never reaches the publish at all.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // GCP Managed Kafka is SASL_SSL/OAUTHBEARER; locally redpanda is PLAINTEXT. The
        // security protocol is an env var so the same binary runs in both contexts —
        // PLAINTEXT (the default) keeps the local stack unchanged. OAUTHBEARER reuses the
        // GcpToken metadata-server/MIG_GCS_TOKEN seam via GcpTokenOauthCallbackHandler; the
        // JAAS login module is required even with a custom handler, else Kafka fails with
        // "No login module found for OAUTHBEARER".
        if (!"PLAINTEXT".equals(env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "OAUTHBEARER");
            props.put("sasl.login.callback.handler.class",
                    "ro.mig.common.GcpTokenOauthCallbackHandler");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        }
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(env("TARGET_SYSTEM_PORT", "8080"));
        double failureRate = Double.parseDouble(env("TARGET_SYSTEM_FAILURE_RATE", "0.15"));
        long seed = Long.parseLong(env("TARGET_SYSTEM_SEED", "20260803"));
        String confirmationBootstrap = env("TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP", "");
        String confirmationTopic = env("TARGET_SYSTEM_CONFIRMATION_TOPIC", "target-system-confirmations");

        TargetSystemMock mock = new TargetSystemMock(failureRate, seed, confirmationBootstrap, confirmationTopic);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v1/accounts", mock::handleAccounts);
        server.createContext("/__admin/stats", mock::handleStats);
        server.createContext("/__admin/reset", mock::handleReset);
        server.createContext("/__admin/suppress-next-confirmation", mock::handleSuppressNextConfirmation);
        server.createContext("/__admin/health", mock::handleHealth);
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();

        // Flush the producer on shutdown so the last confirmations are not lost in the
        // send buffer when the container is signalled to stop between runs.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (mock.producer != null) {
                mock.producer.flush();
                mock.producer.close(Duration.ofSeconds(5));
            }
        }));

        System.out.printf("target-system-mock listening on :%d (injected failure rate %.0f%%, confirmations %s)%n",
                port, failureRate * 100,
                mock.producer == null ? "off" : "→ " + confirmationTopic);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    // ── handlers ─────────────────────────────────────────────────────────────────

    private void handleAccounts(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
            String path = exchange.getRequestURI().getPath();
            String id = path.substring(path.lastIndexOf('/') + 1);
            JsonNode account = accounts.get(id);
            if (account == null) {
                respond(exchange, 404, "{\"error\":\"not_found\"}");
            } else {
                respond(exchange, 200, MAPPER.writeValueAsString(account));
            }
            return;
        }
        if (!"POST".equals(method)) {
            respond(exchange, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }

        received.incrementAndGet();

        // Fail some requests on purpose, before doing any work — the Loader must retry.
        synchronized (random) {
            if (random.nextDouble() < failureRate) {
                injectedFailures.incrementAndGet();
                boolean throttled = random.nextBoolean();
                exchange.getResponseHeaders().add("Retry-After", "1");
                respond(exchange, throttled ? 429 : 503,
                        "{\"error\":\"" + (throttled ? "rate_limited" : "unavailable") + "\"}");
                return;
            }
        }

        byte[] body = exchange.getRequestBody().readAllBytes();
        JsonNode doc;
        try {
            doc = MAPPER.readTree(body);
        } catch (IOException e) {
            respond(exchange, 400, "{\"error\":\"invalid_json\"}");
            return;
        }

        String accountId = doc.path("accountId").asText(null);
        if (accountId == null || accountId.isBlank()) {
            respond(exchange, 422, "{\"error\":\"missing_accountId\"}");
            return;
        }

        String idempotencyKey = exchange.getRequestHeaders().getFirst("X-Idempotency-Key");
        // A blank key is rejected for the same reason, and by the same mechanism, as a
        // blank accountId above. The schema door guarantees dedupKey is non-empty, so this
        // is unreachable in the normal flow — but a hand-edited document or a future
        // producer that skips validation would otherwise collide every record on "" and
        // have them silently swallowed as duplicates of each other. Failing loudly here
        // means the defence does not depend on an upstream door staying shut.
        if (idempotencyKey != null && idempotencyKey.isBlank()) {
            respond(exchange, 422, "{\"error\":\"blank_idempotency_key\"}");
            return;
        }

        if (idempotencyKey != null && idempotency.putIfAbsent(idempotencyKey, accountId) != null) {
            // A replayed batch: acknowledge without creating a second account.
            duplicates.incrementAndGet();
            respond(exchange, 200, "{\"status\":\"duplicate_ignored\",\"accountId\":\"" + accountId + "\"}");
            return;
        }

        accounts.put(accountId, doc);
        accepted.incrementAndGet();
        // RC2: publish the confirmation synchronously *before* the 201 response, so the
        // event is on the topic by the time the loader sees success — and a publish
        // failure is visible here rather than lost past the response. The account key is
        // the idempotency key (== migration.dedupKey, see pipelines/common/mapping.py), so
        // the matcher joins on the same key the dedup already uses. Skipped wholesale
        // when no producer was built (empty bootstrap), which is the no-Kafka path.
        publishConfirmation(doc, accountId, idempotencyKey);
        respond(exchange, 201, "{\"status\":\"created\",\"accountId\":\"" + accountId + "\"}");
    }

    /**
     * Publishes one confirmation event per accepted write. No-op when no producer is
     * configured (empty bootstrap → the no-Kafka path). Returns normally on publish
     * failure: the account was persisted, so the 201 stands; the missing confirmation is
     * what recon will surface as an unconfirmed TARGET row, which is the visible
     * consequence the plan wants rather than a silently swallowed callback error.
     */
    private void publishConfirmation(JsonNode doc, String accountId, String accountKey) {
        if (producer == null) {
            return;
        }
        // RC9: consume the one-shot suppress flag *before* sending. The account is still
        // stored and the 201 still returns; only the publish is skipped.
        if (suppressNextConfirmation.getAndSet(false)) {
            System.out.printf("[mock] suppress-next-confirmation: skipping publish for %s%n", accountKey);
            return;
        }
        String runId = doc.path("migration").path("runId").asText("");
        String json = MAPPER.createObjectNode()
                .put("runId", runId)
                .put("accountId", accountId)
                .put("accountKey", accountKey)
                .put("confirmedAt", Instant.now().toString())
                .toString();
        try {
            producer.send(new ProducerRecord<>(confirmationTopic, accountKey, json)).get();
            confirmationsPublished.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[mock] confirmation publish interrupted for %s%n", accountKey);
        } catch (ExecutionException e) {
            // Visible, not silent: log loudly. recon will report this key as unconfirmed.
            System.err.printf("[mock] confirmation publish FAILED for %s: %s%n",
                    accountKey, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        }
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        ObjectNode stats = MAPPER.createObjectNode();
        stats.put("received", received.get());
        stats.put("accepted", accepted.get());
        stats.put("duplicatesIgnored", duplicates.get());
        stats.put("injectedFailures", injectedFailures.get());
        stats.put("confirmationsPublished", confirmationsPublished.get());
        stats.put("distinctAccounts", accounts.size());
        respond(exchange, 200, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(stats));
    }

    private void handleReset(HttpExchange exchange) throws IOException {
        accounts.clear();
        idempotency.clear();
        received.set(0);
        accepted.set(0);
        duplicates.set(0);
        injectedFailures.set(0);
        confirmationsPublished.set(0);
        suppressNextConfirmation.set(false);
        respond(exchange, 200, "{\"status\":\"reset\"}");
    }

    private void handleSuppressNextConfirmation(HttpExchange exchange) throws IOException {
        // Consume the body so the client can reuse the connection.
        exchange.getRequestBody().readAllBytes();
        suppressNextConfirmation.set(true);
        respond(exchange, 200, "{\"status\":\"suppress-next-confirmation-armed\"}");
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        respond(exchange, 200, "{\"status\":\"ok\"}");
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
        exchange.close();
    }
}
