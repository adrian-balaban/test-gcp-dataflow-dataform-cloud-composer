package ro.mig.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ro.mig.common.Artefacts;
import ro.mig.common.Checksums;
import ro.mig.common.HttpObjectStore;
import ro.mig.common.ObjectStore;
import ro.mig.common.RunContext;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Loader App from architecture diagram's Load lane.
 *
 * <p>
 * "Retrieves the json files and pushes them to Target Loader APIs. Handles
 * retries and error mechanisms." Owned by the other team in reality; this is a
 * faithful mock that honours the stated contract.
 *
 * <p>
 * <b>Two sinks.</b> {@code --sink kafka} (the default) produces every document to
 * the target topic and then settles the run against the confirmation and rejection
 * streams; {@code --sink http} is the original {@code POST /v1/accounts} path, kept
 * for one release so both are runnable side by side against the same acceptance
 * suite (docs/PLAN-CHANGES-02092026-kafka-loader.md §8).
 *
 * <p>
 * <b>Why settling exists.</b> On the HTTP path the response code <em>is</em> the
 * verdict: 201 accepted, 200 duplicate, 4xx permanently rejected. A Kafka produce
 * ack carries none of that — {@code acks=all} means the broker durably holds the
 * bytes, not that Target System parsed, accepted or persisted the record. So the
 * verdict is relocated rather than deleted: after publishing, the loader does a
 * bounded read of the return topics for this run and derives its tallies from what
 * Target System actually said. Anything published but never spoken about is counted
 * as {@code unsettled}, an outcome with no HTTP equivalent, and a non-zero
 * {@code unsettled} fails the run rather than passing quietly.
 *
 * <p>
 * Idempotency is unchanged in substance: {@code migration.dedupKey} was the
 * {@code X-Idempotency-Key} header and is now the message key, so it still both
 * dedupes and — being a hash of the account-key fields — partitions by account,
 * preserving per-account ordering. The {@code accountId}/{@code dedupKey} presence
 * check still routes to {@code .ERR} without sending: that is a batch defect and it
 * predates Kafka.
 *
 * <p>
 * Produces the load-side half of the five artefacts, mirroring the extraction lane:
 * {@code .CHS}, {@code .ERR}, {@code .RPT} and — last, once everything else is
 * durable — the {@code .FLG} semaphore.
 */
public final class LoaderApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Bumped when the {@code .RPT} field set changes shape, so archived evidence stays
     * interpretable rather than silently re-meaning its own fields. Version 1 was the
     * HTTP-only report; version 2 adds {@code published}/{@code unsettled} and redefines
     * {@code accepted} as "confirmed by Target System" on the Kafka sink.
     */
    private static final int REPORT_VERSION = 2;

    public static void main(String[] args) {
        Args a = Args.parse(args);
        RunContext run = new RunContext(a.runId);

        ObjectStore store = HttpObjectStore.fromEnv(a.gcsHost);
        store.createBucket(a.reconBucket);

        String jsonPrefix = "json/" + run.runId() + "/";
        List<String> files = store.list(a.jsonBucket, jsonPrefix);
        if (files.isEmpty()) {
            throw new IllegalStateException(
                    "no JSON files at gs://" + a.jsonBucket + "/" + jsonPrefix
                            + " — did the JSON Data Producer run for this run id?");
        }

        Sink sink = a.sink == SinkKind.KAFKA
                ? new KafkaSink(a.kafkaBootstrap, a.kafkaTopic, a.confirmationTopic,
                        a.rejectionTopic, a.settleTimeoutSeconds, run.runId())
                : new HttpSink(a.targetSystemUrl, a.maxRetries);

        List<LoadError> errors = new ArrayList<>();
        List<Checksums.Entry> checksums = new ArrayList<>();
        long documentsRead = 0;

        try (sink) {
            for (String file : files) {
                byte[] payload = store.get(a.jsonBucket, file);
                checksums.add(new Checksums.Entry(
                        Checksums.sha256(payload), Checksums.countRecords(payload), file));

                for (String line : Checksums.utf8(payload).split("\n")) {
                    if (line.isBlank()) {
                        continue;
                    }
                    documentsRead++;
                    try {
                        JsonNode doc = MAPPER.readTree(line);
                        sink.offer(doc);
                    } catch (LoadFailure e) {
                        errors.add(new LoadError(e.accountId, e.status, e.getMessage(), line));
                    } catch (IOException e) {
                        errors.add(new LoadError("<unparseable>", 0,
                                "invalid JSON: " + e.getMessage(), line));
                    }
                }
            }

            // Everything is offered; now find out what actually happened to it. On the
            // HTTP sink this is a no-op — the verdicts were already collected inline.
            Tally tally = sink.settle();
            for (LoadError e : tally.errors()) {
                errors.add(e);
            }

            writeArtefacts(store, a, run, files, checksums, documentsRead, tally, errors);

            System.out.printf(
                    "loader: run=%s sink=%s files=%d documents=%d published=%d accepted=%d "
                            + "duplicates=%d errors=%d unsettled=%d retries=%d -> gs://%s/%s%n",
                    run.runId(), a.sink.name().toLowerCase(), files.size(), documentsRead,
                    tally.published(), tally.accepted(), tally.duplicates(), errors.size(),
                    tally.unsettled(), tally.retries(), a.reconBucket,
                    Artefacts.prefix(Artefacts.LANE_LOAD, run.runId()));

            if (!errors.isEmpty()) {
                System.err.printf("loader: %d document(s) failed permanently — see %s%n",
                        errors.size(),
                        Artefacts.prefix(Artefacts.LANE_LOAD, run.runId())
                                + Artefacts.err("ACCOUNT"));
            }

            // A published document nobody ever confirmed or rejected is not a success. It
            // is the failure mode Kafka introduces and HTTP could not express, so it is
            // the one that must not be allowed to exit zero: the .RPT records it, the .FLG
            // above still exists for forensics, and the process fails loudly.
            if (tally.unsettled() > 0) {
                System.err.printf(
                        "loader: %d document(s) published but never settled — Target System "
                                + "neither confirmed nor rejected them within %ds. The load "
                                + "cannot be declared complete.%n",
                        tally.unsettled(), a.settleTimeoutSeconds);
                System.exit(1);
            }
        }
    }

    private static void writeArtefacts(ObjectStore store, Args a, RunContext run,
            List<String> files, List<Checksums.Entry> checksums, long documentsRead,
            Tally tally, List<LoadError> errors) {
        String prefix = Artefacts.prefix(Artefacts.LANE_LOAD, run.runId());
        Map<String, byte[]> artefacts = new LinkedHashMap<>();

        artefacts.put(Artefacts.chs("ACCOUNT"), Checksums.utf8(Checksums.render(run, checksums)));
        artefacts.put(Artefacts.err("ACCOUNT"), Checksums.utf8(renderErrors(errors)));
        artefacts.put(Artefacts.rpt("ACCOUNT"),
                renderReport(run, a, files, documentsRead, tally, errors.size()));

        artefacts.forEach((name, body) -> store.put(a.reconBucket, prefix + name, body));

        // Semaphore last — same contract as the extraction lane, in the other direction.
        store.put(a.reconBucket, prefix + Artefacts.flg("ACCOUNT"),
                renderFlag(run, artefacts.keySet()));
    }

    // ── sinks ────────────────────────────────────────────────────────────────────

    private enum SinkKind {
        HTTP, KAFKA
    }

    /**
     * The outcome of a whole load, however it was delivered.
     *
     * <p>
     * {@code published} and {@code unsettled} are meaningful only on the Kafka sink;
     * on HTTP {@code published == accepted + duplicates} and {@code unsettled} is
     * always zero, because a synchronous response settles every document by
     * construction.
     */
    record Tally(long published, long accepted, long duplicates, long unsettled,
            long retries, List<LoadError> errors) {
    }

    private interface Sink extends AutoCloseable {
        /** Deliver one document, or throw {@link LoadFailure} to route it to {@code .ERR}. */
        void offer(JsonNode doc);

        /** Resolve every offered document into a final verdict. */
        Tally settle();

        @Override
        void close();
    }

    record LoadError(String accountId, int status, String reason, String raw) {
    }

    private static final class LoadFailure extends RuntimeException {
        final String accountId;
        final int status;

        LoadFailure(String accountId, int status, String message) {
            super(message);
            this.accountId = accountId;
            this.status = status;
        }
    }

    /**
     * Reject a document whose required identity field is absent or blank.
     *
     * <p>
     * Thrown as a {@link LoadFailure} so it lands in {@code .ERR} with the raw line
     * and is counted in {@code errors} — the same route as any other permanent
     * failure. The document is never sent, so it cannot corrupt server-side
     * idempotency state. Both fields were previously defaulted ("&lt;missing&gt;" and
     * ""), which turned a malformed batch into silent data loss: an empty idempotency
     * key is accepted as a <em>valid</em> key, so the first key-less document is
     * created and every subsequent one collides with it and counts as a duplicate.
     * N-1 accounts would vanish with a zero exit code and a {@code .FLG} claiming
     * success. This is unchanged by the move to Kafka — a blank message key partitions
     * arbitrarily and defeats the dedupe just as thoroughly as a blank header did.
     */
    private static String requireField(JsonNode doc, String path, JsonNode value) {
        String text = value.isMissingNode() || value.isNull() ? "" : value.asText("");
        if (text.isBlank()) {
            throw new LoadFailure(
                    doc.path("accountId").asText("<unidentified>"), 0,
                    "missing required field '" + path + "' — refusing to send a document "
                            + "with no identity; an empty idempotency key would silently "
                            + "collide with every other key-less document");
        }
        return text;
    }

    // ── kafka sink ───────────────────────────────────────────────────────────────

    /**
     * Publish every document to the target topic, then settle the run against the two
     * return topics.
     *
     * <p>
     * The producer is configured once in {@link ro.mig.common.KafkaClients}: idempotent,
     * {@code acks=all}, five retries. That combination is what replaced the old backoff
     * loop — a retried send cannot produce a duplicate, so retry stops being application
     * code and the {@code retriesPerformed} tally becomes producer-internal and is
     * reported as zero.
     */
    private static final class KafkaSink implements Sink {
        private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
        private final String topic;
        private final String confirmationTopic;
        private final String rejectionTopic;
        private final String bootstrap;
        private final int settleTimeoutSeconds;
        private final String runId;
        /**
         * Every key we successfully handed to the broker, in publish order. The settle
         * phase is a set-difference against this: confirmed, rejected, and whatever is
         * left over is unsettled.
         */
        private final Set<String> published = new LinkedHashSet<>();
        /** Raw line per key, so a rejection can carry the document that caused it into .ERR. */
        private final Map<String, String> rawByKey = new LinkedHashMap<>();
        private final Map<String, String> accountIdByKey = new LinkedHashMap<>();

        KafkaSink(String bootstrap, String topic, String confirmationTopic,
                String rejectionTopic, int settleTimeoutSeconds, String runId) {
            this.bootstrap = bootstrap;
            this.topic = topic;
            this.confirmationTopic = confirmationTopic;
            this.rejectionTopic = rejectionTopic;
            this.settleTimeoutSeconds = settleTimeoutSeconds;
            this.runId = runId;
            this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(
                    ro.mig.common.KafkaClients.producerProps(bootstrap));
        }

        @Override
        public void offer(JsonNode doc) {
            String accountId = requireField(doc, "accountId", doc.path("accountId"));
            String dedupKey = requireField(
                    doc, "migration.dedupKey", doc.path("migration").path("dedupKey"));

            // dedupKey is account_key — a sha256 over the mapping's account-key fields
            // (pipelines/common/mapping.py) — so keying by it both dedupes and partitions
            // by account, which is what preserves per-account ordering. Same key the
            // Python KafkaTargetWriter already uses, so the two producers agree.
            org.apache.kafka.clients.producer.ProducerRecord<String, String> record =
                    new org.apache.kafka.clients.producer.ProducerRecord<>(
                            topic, dedupKey, doc.toString());
            // Same three headers, same names, as the Python sink.
            record.headers().add("run-id", runId.getBytes(StandardCharsets.UTF_8));
            record.headers().add("idempotency-key", dedupKey.getBytes(StandardCharsets.UTF_8));
            record.headers().add("batch-id", runId.getBytes(StandardCharsets.UTF_8));

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // Not swallowed: the key never enters `published`, so settle() cannot
                    // count it as confirmed, and the run fails on the arithmetic below.
                    System.err.printf("loader: publish FAILED for %s: %s%n",
                            dedupKey, exception.getMessage());
                }
            });
            published.add(dedupKey);
            rawByKey.put(dedupKey, doc.toString());
            accountIdByKey.put(dedupKey, accountId);
        }

        @Override
        public Tally settle() {
            // flush() returns void but guarantees every send has completed or failed; the
            // callbacks above have therefore all run by the time this returns. This project
            // has already lost 400 records once to treating an unflushed producer as a slow
            // success (pipelines/common/sinks.py), so the flush is not optional.
            producer.flush();

            // Both topics, one loop, bounded by "every published key has a verdict"
            // rather than by the topics' end offsets — see ReturnStream for why the
            // end-offset form silently under-reports here.
            ReturnStream.Verdicts verdicts = ReturnStream.settle(
                    bootstrap, confirmationTopic, rejectionTopic, runId, "loader",
                    published, settleTimeoutSeconds);

            return settleAgainst(published, verdicts.confirmed(), verdicts.duplicates(),
                    verdicts.rejected(), accountIdByKey, rawByKey);
        }

        @Override
        public void close() {
            producer.close(Duration.ofSeconds(30));
        }
    }

    /**
     * The settle set-difference, as a pure function of the three key sets.
     *
     * <p>
     * This is the arithmetic that replaces the HTTP status code, so it is extracted from
     * the broker plumbing and made {@code static} for the same reason
     * {@code ReconService.matchConfirmations} is: the interesting failure modes here are
     * off-by-one and double-counting, and neither needs Kafka to reproduce.
     *
     * <p>
     * Three rules, all of them load-bearing:
     * <ul>
     * <li>Only keys we published this run count. A topic in steady state carries other
     * runs' traffic; the {@code runId} filter drops most of it and this intersection
     * drops the rest.
     * <li>A key confirmed <em>and</em> rejected counts as rejected. Preferring the
     * confirmation would hide a real defect behind an optimistic count.
     * <li>Whatever is in neither set is {@code unsettled} — published, and never spoken
     * about. That number failing the run is the whole point of settling.
     * </ul>
     */
    static Tally settleAgainst(Set<String> published, Set<String> confirmed,
            Set<String> duplicates, Map<String, String> rejected,
            Map<String, String> accountIdByKey, Map<String, String> rawByKey) {
        Set<String> confirmedKeys = new LinkedHashSet<>(confirmed);
        Set<String> duplicateKeys = new LinkedHashSet<>(duplicates);
        Map<String, String> rejectedKeys = new LinkedHashMap<>(rejected);

        confirmedKeys.retainAll(published);
        duplicateKeys.retainAll(published);
        rejectedKeys.keySet().retainAll(published);
        confirmedKeys.removeAll(rejectedKeys.keySet());
        duplicateKeys.removeAll(rejectedKeys.keySet());
        // A key reported as both created and duplicate counts once, as a duplicate: the
        // stronger claim is that it already existed. Without this the two sets could
        // overlap and drive `unsettled` negative.
        confirmedKeys.removeAll(duplicateKeys);

        List<LoadError> errors = new ArrayList<>();
        for (Map.Entry<String, String> e : rejectedKeys.entrySet()) {
            errors.add(new LoadError(
                    accountIdByKey.getOrDefault(e.getKey(), "<unidentified>"),
                    0,
                    // Target System's own reason string, in place of an HTTP status.
                    "rejected by target system: " + e.getValue(),
                    rawByKey.getOrDefault(e.getKey(), "")));
        }

        long unsettled = (long) published.size()
                - confirmedKeys.size() - duplicateKeys.size() - rejectedKeys.size();
        return new Tally(published.size(), confirmedKeys.size(), duplicateKeys.size(),
                unsettled, 0, errors);
    }

    // ── http sink (legacy, --sink http) ──────────────────────────────────────────

    /**
     * The original synchronous path: {@code POST /v1/accounts} with
     * {@code X-Idempotency-Key}, hand-rolled retry with backoff and jitter. Retained for
     * one release so the Kafka sink can be compared against it on the same acceptance
     * suite; the response code is the verdict, so {@code settle()} has nothing left to do.
     */
    private static final class HttpSink implements Sink {
        private final HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10)).build();
        private final String baseUrl;
        private final int maxRetries;
        /**
         * Cloud Run authorises with an <em>identity</em> token whose audience is the
         * service URL. Null against the local mock (and off GCP), which wants no auth at
         * all — fetched once here rather than per request, since a load runs well inside
         * the token's lifetime.
         */
        private final String idToken;
        private long retries = 0;
        private long accepted = 0;
        private long duplicates = 0;

        HttpSink(String baseUrl, int maxRetries) {
            this.baseUrl = baseUrl.replaceAll("/+$", "");
            this.maxRetries = maxRetries;
            this.idToken = this.baseUrl.startsWith("https://")
                    ? ro.mig.common.GcpToken.identityToken(this.baseUrl)
                    : null;
        }

        @Override
        public void offer(JsonNode doc) {
            String accountId = requireField(doc, "accountId", doc.path("accountId"));
            String idempotencyKey = requireField(
                    doc, "migration.dedupKey", doc.path("migration").path("dedupKey"));
            byte[] body = doc.toString().getBytes(StandardCharsets.UTF_8);
            send(accountId, idempotencyKey, body);
        }

        @Override
        public Tally settle() {
            // Nothing to do: on HTTP the response code already settled every document.
            return new Tally(accepted + duplicates, accepted, duplicates, 0, retries,
                    List.of());
        }

        @Override
        public void close() {
            // HttpClient needs no shutdown.
        }

        private void send(String accountId, String idempotencyKey, byte[] body) {
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                HttpRequest.Builder builder = HttpRequest
                        .newBuilder(URI.create(baseUrl + "/v1/accounts"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .header("X-Idempotency-Key", idempotencyKey)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
                if (idToken != null) {
                    builder.header("Authorization", "Bearer " + idToken);
                }

                HttpResponse<String> response;
                try {
                    response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
                } catch (IOException | InterruptedException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        throw new LoadFailure(accountId, 0, "interrupted");
                    }
                    if (attempt == maxRetries) {
                        throw new LoadFailure(accountId, 0, "transport failure: " + e.getMessage());
                    }
                    backoff(attempt);
                    continue;
                }

                int status = response.statusCode();
                if (status == 200) {
                    duplicates++; // idempotent replay, acknowledged not re-created
                    return;
                }
                if (status == 201) {
                    accepted++;
                    return;
                }
                // 429 and 5xx are transient; everything else is the document's fault and
                // will fail identically forever, so it goes straight to .ERR.
                boolean transientFailure = status == 429 || status >= 500;
                if (!transientFailure) {
                    throw new LoadFailure(accountId, status,
                            "permanent rejection: HTTP " + status + " " + response.body());
                }
                if (attempt == maxRetries) {
                    throw new LoadFailure(accountId, status,
                            "still failing after " + maxRetries + " retries: HTTP " + status);
                }
                backoff(attempt);
            }
            throw new LoadFailure(accountId, 0, "retry loop exhausted");
        }

        /**
         * Exponential backoff with jitter, so a throttled batch does not retry in
         * lockstep.
         */
        private void backoff(int attempt) {
            retries++;
            long base = Math.min(1000L << attempt, 8000L);
            long sleep = base / 2 + ThreadLocalRandom.current().nextLong(base / 2 + 1);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ── artefacts ────────────────────────────────────────────────────────────────

    private static String renderErrors(List<LoadError> errors) {
        StringBuilder sb = new StringBuilder("# MIG 000001-1 load errors\n");
        sb.append("# accountId\tstatus\treason\traw\n");
        for (LoadError e : errors) {
            sb.append(e.accountId()).append('\t').append(e.status()).append('\t')
                    .append(e.reason()).append('\t').append(e.raw()).append('\n');
        }
        return sb.toString();
    }

    private static byte[] renderReport(RunContext run, Args a, List<String> files, long read,
            Tally tally, int errors) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("lane", Artefacts.LANE_LOAD);
        root.put("record", "ACCOUNT");
        root.put("runId", run.runId());
        // Q3 in the plan: `accepted` keeps its name but changes meaning on the Kafka sink
        // (HTTP 201 count → confirmed-by-Target-System count). Renaming it would break the
        // stated project value that archived evidence reports stay comparable; versioning
        // the report says the same thing without moving the field. `sink` records which
        // definition applies to this particular file.
        root.put("reportVersion", REPORT_VERSION);
        root.put("sink", a.sink.name().toLowerCase());
        root.put("generatedAt", Instant.now().toString());
        root.put("jsonFilesRead", files.size());
        root.put("documentsRead", read);
        root.put("published", tally.published());
        root.put("accepted", tally.accepted());
        root.put("duplicatesIgnored", tally.duplicates());
        root.put("errors", errors);
        root.put("unsettled", tally.unsettled());
        root.put("retriesPerformed", tally.retries());
        ArrayNode names = root.putArray("sourceFiles");
        files.forEach(names::add);
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
        } catch (Exception e) {
            throw new IllegalStateException("could not render load .RPT", e);
        }
    }

    private static byte[] renderFlag(RunContext run, Iterable<String> artefacts) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("runId", run.runId());
        root.put("completedAt", Instant.now().toString());
        ArrayNode names = root.putArray("artefacts");
        artefacts.forEach(names::add);
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
        } catch (Exception e) {
            throw new IllegalStateException("could not render load .FLG", e);
        }
    }

    // ── arguments ────────────────────────────────────────────────────────────────

    private static final class Args {
        String jsonBucket = "mig-json-out";
        String reconBucket = "mig-recon";
        String gcsHost = "http://localhost:4443";
        String targetSystemUrl = "http://localhost:8080";
        String runId = "";
        int maxRetries = 5;
        SinkKind sink = SinkKind.KAFKA;
        String kafkaBootstrap = "";
        String kafkaTopic = "target-system-target";
        String confirmationTopic = "target-system-confirmations";
        String rejectionTopic = "target-system-rejections";
        // 30s is fine for 400 documents and wrong for millions; the plan calls this out as
        // a new operational parameter with no HTTP analogue (Q5). 120s is a default, not an
        // answer — the DAG passes an explicit value.
        int settleTimeoutSeconds = 120;

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length - 1; i += 2) {
                String value = argv[i + 1];
                switch (argv[i]) {
                    case "--json-bucket" -> a.jsonBucket = value;
                    case "--recon-bucket" -> a.reconBucket = value;
                    case "--gcs-host" -> a.gcsHost = value;
                    case "--target-system-url" -> a.targetSystemUrl = value;
                    case "--run-id" -> a.runId = value;
                    case "--max-retries" -> a.maxRetries = Integer.parseInt(value);
                    case "--sink" -> a.sink = switch (value) {
                        case "http" -> SinkKind.HTTP;
                        case "kafka" -> SinkKind.KAFKA;
                        default -> throw new IllegalArgumentException(
                                "--sink must be 'http' or 'kafka', got: " + value);
                    };
                    case "--kafka-bootstrap" -> a.kafkaBootstrap = value;
                    case "--kafka-topic" -> a.kafkaTopic = value;
                    case "--confirmation-topic" -> a.confirmationTopic = value;
                    case "--rejection-topic" -> a.rejectionTopic = value;
                    case "--settle-timeout-seconds" ->
                        a.settleTimeoutSeconds = Integer.parseInt(value);
                    default -> throw new IllegalArgumentException("unknown option: " + argv[i]);
                }
            }
            if (a.runId.isBlank()) {
                throw new IllegalArgumentException("--run-id is required");
            }
            // Fail here rather than 60s later inside the producer's metadata fetch, where
            // a missing bootstrap reads as "Topic not present in metadata after 60000 ms"
            // and looks like a broker problem instead of a missing argument.
            if (a.sink == SinkKind.KAFKA && a.kafkaBootstrap.isBlank()) {
                throw new IllegalArgumentException(
                        "--kafka-bootstrap is required when --sink kafka (the default). "
                                + "Pass --sink http to use the legacy POST /v1/accounts path.");
            }
            return a;
        }
    }
}
