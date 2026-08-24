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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Loader App from architecture diagram's Load lane.
 *
 * <p>
 * "Retrieves the json files and pushes them to Target Loader APIs. Handles
 * retries and error
 * mechanisms." Owned by the other team in reality; this is a faithful mock that
 * honours the
 * stated contract.
 *
 * <p>
 * Two things here are load-bearing rather than decorative:
 *
 * <ul>
 * <li><b>Idempotency.</b> Every request carries {@code X-Idempotency-Key} set
 * to the engine's
 * deterministic dedup key. That is what makes at-least-once delivery safe: a
 * replayed
 * batch is acknowledged without creating a second account.
 * <li><b>Retry with backoff and jitter.</b> The Target System mock injects 429s
 * and 503s on
 * purpose, so this path is exercised on every run rather than only in theory.
 * 4xx other
 * than 429 is treated as permanent and routed to {@code .ERR} — retrying a
 * malformed
 * document forever is how a migration silently stalls.
 * </ul>
 *
 * <p>
 * Produces the load-side half of the five artefacts, mirroring the extraction
 * lane:
 * {@code .CHS}, {@code .ERR}, {@code .RPT} and — last, once everything else is
 * durable — the
 * {@code .FLG} semaphore.
 */
public final class LoaderApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

        Loader loader = new Loader(a.targetSystemUrl, a.maxRetries);
        List<LoadError> errors = new ArrayList<>();
        List<Checksums.Entry> checksums = new ArrayList<>();
        long documentsRead = 0;
        long accepted = 0;
        long duplicates = 0;

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
                    Outcome outcome = loader.push(doc);
                    if (outcome == Outcome.DUPLICATE) {
                        duplicates++;
                    } else {
                        accepted++;
                    }
                } catch (LoadFailure e) {
                    errors.add(new LoadError(e.accountId, e.status, e.getMessage(), line));
                } catch (IOException e) {
                    errors.add(new LoadError("<unparseable>", 0, "invalid JSON: " + e.getMessage(), line));
                }
            }
        }

        String prefix = Artefacts.prefix(Artefacts.LANE_LOAD, run.runId());
        Map<String, byte[]> artefacts = new LinkedHashMap<>();

        artefacts.put(Artefacts.chs("ACCOUNT"), Checksums.utf8(Checksums.render(run, checksums)));
        artefacts.put(Artefacts.err("ACCOUNT"), Checksums.utf8(renderErrors(errors)));
        artefacts.put(Artefacts.rpt("ACCOUNT"), renderReport(
                run, files, documentsRead, accepted, duplicates, errors.size(), loader));

        artefacts.forEach((name, body) -> store.put(a.reconBucket, prefix + name, body));

        // Semaphore last — same contract as the extraction lane, in the other
        // direction.
        store.put(a.reconBucket, prefix + Artefacts.flg("ACCOUNT"),
                renderFlag(run, artefacts.keySet()));

        System.out.printf(
                "loader: run=%s files=%d documents=%d accepted=%d duplicates=%d errors=%d "
                        + "retries=%d -> gs://%s/%s%n",
                run.runId(), files.size(), documentsRead, accepted, duplicates, errors.size(),
                loader.retries, a.reconBucket, prefix);

        if (!errors.isEmpty()) {
            System.err.printf("loader: %d document(s) failed permanently — see %s%n",
                    errors.size(), prefix + Artefacts.err("ACCOUNT"));
        }
    }

    // ── pushing ──────────────────────────────────────────────────────────────────

    private enum Outcome {
        CREATED, DUPLICATE
    }

    private record LoadError(String accountId, int status, String reason, String raw) {
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

    private static final class Loader {
        private final HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10)).build();
        private final String baseUrl;
        private final int maxRetries;
        /**
         * Cloud Run authorises with an <em>identity</em> token whose audience is the
         * service
         * URL. Null against the local mock (and off GCP), which wants no auth at all —
         * fetched once here rather than per request, since a load runs well inside the
         * token's lifetime.
         */
        private final String idToken;
        long retries = 0;

        Loader(String baseUrl, int maxRetries) {
            this.baseUrl = baseUrl.replaceAll("/+$", "");
            this.maxRetries = maxRetries;
            this.idToken = this.baseUrl.startsWith("https://")
                    ? ro.mig.common.GcpToken.identityToken(this.baseUrl)
                    : null;
        }

        Outcome push(JsonNode doc) {
            // Both of these were previously defaulted ("<missing>" and ""), which turned a
            // malformed batch into silent data loss: an empty idempotency key is accepted
            // by the server as a *valid* key, so the first key-less document is created
            // and every subsequent one collides with it and is counted as a duplicate.
            // N-1 accounts would vanish with a zero exit code and a .FLG claiming success.
            // A missing key is a defect in the batch, so it goes to .ERR like any other.
            String accountId = requireField(doc, "accountId", doc.path("accountId"));
            String idempotencyKey = requireField(
                    doc, "migration.dedupKey", doc.path("migration").path("dedupKey"));
            byte[] body = doc.toString().getBytes(StandardCharsets.UTF_8);

            return send(accountId, idempotencyKey, body);
        }

        /**
         * Reject a document whose required identity field is absent or blank.
         *
         * <p>
         * Thrown as a {@link LoadFailure} so it lands in {@code .ERR} with the raw line
         * and is counted in {@code errors} — the same route as any other permanent
         * failure.
         * The document is never sent, so it cannot corrupt server-side idempotency
         * state.
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

        private Outcome send(String accountId, String idempotencyKey, byte[] body) {
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/accounts"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .header("X-Idempotency-Key", idempotencyKey)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
                if (idToken != null) {
                    builder.header("Authorization", "Bearer " + idToken);
                }
                HttpRequest request = builder.build();

                HttpResponse<String> response;
                try {
                    response = http.send(request, HttpResponse.BodyHandlers.ofString());
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
                    return Outcome.DUPLICATE; // idempotent replay, acknowledged not re-created
                }
                if (status == 201) {
                    return Outcome.CREATED;
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

    private static byte[] renderReport(RunContext run, List<String> files, long read,
            long accepted, long duplicates, int errors, Loader loader) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("lane", Artefacts.LANE_LOAD);
        root.put("record", "ACCOUNT");
        root.put("runId", run.runId());
        root.put("generatedAt", Instant.now().toString());
        root.put("jsonFilesRead", files.size());
        root.put("documentsRead", read);
        root.put("accepted", accepted);
        root.put("duplicatesIgnored", duplicates);
        root.put("errors", errors);
        root.put("retriesPerformed", loader.retries);
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
                    default -> throw new IllegalArgumentException("unknown option: " + argv[i]);
                }
            }
            if (a.runId.isBlank()) {
                throw new IllegalArgumentException("--run-id is required");
            }
            return a;
        }
    }
}
