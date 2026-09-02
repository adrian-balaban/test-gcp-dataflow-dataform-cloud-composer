package ro.mig.vault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import ro.mig.common.KafkaClients;

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
 * <p><b>Two intakes.</b> The POST endpoint above, and — when
 * {@code TARGET_SYSTEM_TARGET_TOPIC} is set — a consumer on the Loader's target topic
 * (docs/PLAN-CHANGES-02092026-kafka-loader.md). Both funnel into the same idempotency map,
 * so a document delivered twice by different transports still creates one account. On the
 * Kafka intake there is no response code to return, so the verdict is published instead:
 * a confirmation event per accepted document, a rejection event per refused one. The
 * rejection topic is what gives the Loader's {@code .ERR} a source; without it a bad
 * document is indistinguishable from a slow one and lands in {@code unsettled}.
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

    private final AtomicLong rejectionsPublished = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();

    private final double failureRate;
    private final Random random;
    // Null when no bootstrap is configured → every confirmation path is a no-op.
    private final KafkaProducer<String, String> producer;
    private final String confirmationTopic;
    private final String rejectionTopic;

    public TargetSystemMock(double failureRate, long seed) {
        this(failureRate, seed, null, null, null);
    }

    public TargetSystemMock(double failureRate, long seed,
                            String confirmationBootstrap, String confirmationTopic,
                            String rejectionTopic) {
        this.failureRate = failureRate;
        this.random = new Random(seed);
        this.confirmationTopic = confirmationTopic;
        this.rejectionTopic = rejectionTopic;
        this.producer = confirmationBootstrap == null || confirmationBootstrap.isBlank()
                ? null
                : buildProducer(confirmationBootstrap);
    }

    private static KafkaProducer<String, String> buildProducer(String bootstrap) {
        // The confirmation stream is tiny and per-run; the sends below are made synchronous
        // at the call site so a broker failure is visible on the call that caused it rather
        // than in a callback the handler has already returned past. The property set —
        // including the KAFKA_SECURITY_PROTOCOL → SASL_SSL/OAUTHBEARER block for GCP
        // Managed Kafka — is shared with the loader and recon via ro.mig.common.KafkaClients
        // so all three producers on this project cannot drift apart.
        return new KafkaProducer<>(KafkaClients.producerProps(bootstrap));
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(env("TARGET_SYSTEM_PORT", "8080"));
        double failureRate = Double.parseDouble(env("TARGET_SYSTEM_FAILURE_RATE", "0.15"));
        long seed = Long.parseLong(env("TARGET_SYSTEM_SEED", "20260803"));
        String confirmationBootstrap = env("TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP", "");
        String confirmationTopic = env("TARGET_SYSTEM_CONFIRMATION_TOPIC", "target-system-confirmations");
        String rejectionTopic = env("TARGET_SYSTEM_REJECTION_TOPIC", "target-system-rejections");
        // Empty → no consumer thread, and the mock is HTTP-only exactly as before. Set on
        // the Kafka path so the mock consumes what the Loader produces.
        String targetTopic = env("TARGET_SYSTEM_TARGET_TOPIC", "");

        TargetSystemMock mock = new TargetSystemMock(
                failureRate, seed, confirmationBootstrap, confirmationTopic, rejectionTopic);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v1/accounts", mock::handleAccounts);
        server.createContext("/__admin/stats", mock::handleStats);
        server.createContext("/__admin/reset", mock::handleReset);
        server.createContext("/__admin/suppress-next-confirmation", mock::handleSuppressNextConfirmation);
        server.createContext("/__admin/health", mock::handleHealth);
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();

        // The consumer runs on its own thread so the HTTP endpoints stay responsive; both
        // sinks can be live at once, which is what lets `--sink http` and `--sink kafka`
        // be compared against the same running mock.
        AtomicBoolean running = new AtomicBoolean(true);
        if (!targetTopic.isBlank() && !confirmationBootstrap.isBlank()) {
            Thread consumer = new Thread(
                    () -> mock.consumeLoop(confirmationBootstrap, targetTopic, running),
                    "target-topic-consumer");
            consumer.setDaemon(true);
            consumer.start();
        }

        // Flush the producer on shutdown so the last confirmations are not lost in the
        // send buffer when the container is signalled to stop between runs.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            if (mock.producer != null) {
                mock.producer.flush();
                mock.producer.close(Duration.ofSeconds(5));
            }
        }));

        System.out.printf("target-system-mock listening on :%d (injected failure rate %.0f%%, "
                        + "confirmations %s, consuming %s)%n",
                port, failureRate * 100,
                mock.producer == null ? "off" : "→ " + confirmationTopic,
                targetTopic.isBlank() ? "off (HTTP only)" : targetTopic);
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
        // NOTE: the Kafka consumer path below reaches the same state through applyDocument;
        // the two must stay in step, which is why both go through the one idempotency map.
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
    /**
     * The two confirmation outcomes. Both mean "this key is applied at Target System" and
     * both settle the document; they differ only in whether <em>this</em> delivery is what
     * applied it. Carried as a field rather than as a separate topic so the loader needs
     * one subscription and the ordering between "created" and a later "duplicate" for the
     * same key is preserved by the partition.
     */
    private static final String OUTCOME_CREATED = "created";
    private static final String OUTCOME_DUPLICATE = "duplicate";

    private void publishConfirmation(JsonNode doc, String accountId, String accountKey) {
        publishConfirmation(doc, accountId, accountKey, OUTCOME_CREATED);
    }

    private void publishConfirmation(JsonNode doc, String accountId, String accountKey,
                                     String outcome) {
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
                // Absent on events written before this field existed; consumers default it
                // to "created", which is what every such event meant.
                .put("outcome", outcome)
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

    // ── kafka intake ─────────────────────────────────────────────────────────────

    /**
     * Consumes the loader's target topic and applies each document exactly as the POST
     * handler would.
     *
     * <p>
     * This is the counterpart to the Loader moving off HTTP
     * (docs/PLAN-CHANGES-02092026-kafka-loader.md, change #5). Every accepted document
     * publishes a confirmation and every refused one publishes a rejection, because on
     * the Kafka path those two topics <em>are</em> the verdict — without a rejection the
     * loader cannot tell a bad document from a slow one, and a defect would be reported
     * as {@code unsettled} rather than as an error with a reason.
     *
     * <p>
     * The consumer group is stable ({@code target-system-mock}) rather than per-run: this
     * is a long-lived service consuming a stream, not a one-shot bounded read, so it
     * should resume where it left off after a restart instead of reprocessing the topic.
     * Reprocessing would be harmless — the idempotency map makes apply idempotent — but it
     * would republish confirmations and inflate the counters.
     *
     * <p>
     * The injected-failure knob still applies, as lag rather than as a 429: a throttled
     * document is simply left unacknowledged for a beat. There is no backpressure signal
     * to send on this path, which is precisely the tradeoff the plan documents.
     */
    private void consumeLoop(String bootstrap, String topic, AtomicBoolean running) {
        Properties props = KafkaClients.consumerProps(bootstrap, "target-system-mock");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.List.of(topic));
            System.out.printf("[mock] consuming %s → confirmations=%s rejections=%s%n",
                    topic, confirmationTopic, rejectionTopic);
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    consumed.incrementAndGet();
                    applyDocument(record.key(), record.value());
                }
                // Commit only after the batch has been applied and its verdicts published,
                // so a crash mid-batch replays rather than silently skips. At-least-once is
                // safe here precisely because apply is idempotent on the dedup key.
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            // Loud, and fatal to the loop rather than to the process: the HTTP endpoints
            // stay up so /__admin/stats is still readable while diagnosing.
            System.err.printf("[mock] consumer loop stopped: %s%n", e.getMessage());
        }
    }

    /**
     * The Kafka-side equivalent of one POST. Returns nothing: the verdict goes onto the
     * return topics instead of into a response code.
     */
    private void applyDocument(String dedupKey, String body) {
        received.incrementAndGet();

        JsonNode doc;
        try {
            doc = MAPPER.readTree(body);
        } catch (IOException e) {
            publishRejection(null, dedupKey, "<unparseable>", "invalid_json");
            return;
        }

        String accountId = doc.path("accountId").asText(null);
        if (accountId == null || accountId.isBlank()) {
            publishRejection(doc, dedupKey, "<unidentified>", "missing_accountId");
            return;
        }
        // Mirrors the blank-key check on the POST path, and for the same reason: a blank
        // key collides every record onto one entry and they vanish as duplicates of each
        // other. On this path there is no 422 to return, so it becomes a rejection event.
        if (dedupKey == null || dedupKey.isBlank()) {
            publishRejection(doc, dedupKey, accountId, "blank_idempotency_key");
            return;
        }

        if (idempotency.putIfAbsent(dedupKey, accountId) != null) {
            // A replayed batch: no second account, but still a verdict. On HTTP a replay
            // returned 200 — a positive answer the loader counted as a duplicate and moved
            // on. Here there is no response, so staying silent would leave the replayed
            // document with no verdict at all and the loader would report a re-run of a
            // perfectly good batch as 100% `unsettled` and fail it.
            //
            // The event carries outcome=duplicate rather than outcome=created, so the
            // loader can still tell "newly applied" from "already applied" and keep
            // reporting `duplicatesIgnored` with the meaning it had on the HTTP path
            // (plan Q2). Both outcomes settle the document; only the tally differs.
            duplicates.incrementAndGet();
            publishConfirmation(doc, accountId, dedupKey, OUTCOME_DUPLICATE);
            return;
        }

        accounts.put(accountId, doc);
        accepted.incrementAndGet();
        publishConfirmation(doc, accountId, dedupKey);
    }

    /**
     * Publishes one rejection event, carrying the reason string that becomes the
     * {@code .ERR} row's reason in place of an HTTP status code. Synchronous for the same
     * reason confirmations are: a publish failure must be visible here, not lost in a
     * callback.
     */
    private void publishRejection(JsonNode doc, String dedupKey, String accountId, String reason) {
        System.err.printf("[mock] rejecting %s (%s): %s%n", accountId, dedupKey, reason);
        if (producer == null || rejectionTopic == null || rejectionTopic.isBlank()) {
            return;
        }
        String runId = doc == null ? "" : doc.path("migration").path("runId").asText("");
        String json = MAPPER.createObjectNode()
                .put("runId", runId)
                .put("accountId", accountId)
                .put("accountKey", dedupKey == null ? "" : dedupKey)
                .put("reason", reason)
                .put("rejectedAt", Instant.now().toString())
                .toString();
        try {
            producer.send(new ProducerRecord<>(rejectionTopic, dedupKey, json)).get();
            rejectionsPublished.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[mock] rejection publish interrupted for %s%n", dedupKey);
        } catch (ExecutionException e) {
            // Visible, not silent. The loader will report this document as unsettled
            // rather than as an error, which is a weaker but still failing outcome.
            System.err.printf("[mock] rejection publish FAILED for %s: %s%n",
                    dedupKey, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        }
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        ObjectNode stats = MAPPER.createObjectNode();
        stats.put("received", received.get());
        stats.put("accepted", accepted.get());
        stats.put("duplicatesIgnored", duplicates.get());
        stats.put("injectedFailures", injectedFailures.get());
        stats.put("confirmationsPublished", confirmationsPublished.get());
        stats.put("rejectionsPublished", rejectionsPublished.get());
        stats.put("consumedFromTopic", consumed.get());
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
        rejectionsPublished.set(0);
        consumed.set(0);
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
