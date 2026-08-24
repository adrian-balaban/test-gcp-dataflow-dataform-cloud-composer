package ro.mig.recon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import ro.mig.common.Artefacts;
import ro.mig.common.BigQueryRest;
import ro.mig.common.HttpObjectStore;
import ro.mig.common.ObjectStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Reconciliation Services — the box architecture diagram hangs off both
 * BigQuery datasets.
 *
 * <p>
 * Performs the two reconciliations the diagram names, then writes the
 * migrability and
 * reconciliability reports:
 *
 * <ul>
 * <li><b>Source Reconciliation</b> — what the Extractor said it produced (the
 * {@code .RPT}
 * artefact) against what actually landed in the Extraction dataset.
 * <li><b>Transformation/Load Reconciliation</b> — the Transformation dataset
 * against what the
 * Loader reported pushing to Target System.
 * <li><b>Target System Reconciliation</b> — every TARGET row the Loader reported
 * pushing,
 * checked against the confirmation events Target System published back on the
 * confirmation
 * stream (docs/PLAN-CHANGES-22082026.md). Closes the gap between "the Loader got a
 * 201" and
 * "Target System actually persisted the row": a set-difference join on account_key
 * names any
 * TARGET row with no matching confirmation. Skipped (reported as
 * {@code enabled=false}) when
 * no confirmation bootstrap is configured, which is what keeps a no-Kafka run green.
 * </ul>
 *
 * <p>
 * The verdict is the balancing equation, evaluated across the whole lane:
 *
 * <pre>
 * SRC_read = TARGET_written + rejected
 * </pre>
 *
 * <p>
 * A run that does not balance is a failed run — this exits non-zero, which is
 * what fails
 * the Composer DAG. Reconciliation is off the data path (it verifies after the
 * fact), so it
 * compares aggregates and keys rather than streaming records.
 */
public final class ReconService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        Args a = Args.parse(args);
        ObjectStore store = HttpObjectStore.fromEnv(a.gcsHost);
        BigQueryRest bq = BigQueryRest.fromEnv(a.bqHost, a.project);

        // The run id is bound as a query parameter, never interpolated: it arrives from
        // the CLI, and BigQuery's jobs.query accepts arbitrary SELECT, so interpolation
        // was a live injection path. Dataset and project names stay inline because SQL
        // parameters cannot bind identifiers — they are validated at Args.parse
        // instead.
        String q = "@run_id";
        Map<String, String> qp = Map.of("run_id", a.runId);

        // ── source reconciliation ────────────────────────────────────────────────
        //
        // The Extractor's .RPT is read from the recon bucket, not the landing bucket:
        // in
        // the landing bucket it is sealed inside the PGP bundle, and reconciliation has
        // no business holding a decryption key. The File Processor republishes it in
        // the
        // clear once it has verified the checksums.
        JsonNode extractRpt = readReport(store, a.reconBucket, a.runId, Artefacts.LANE_EXTRACTION);
        long srcExtracted = extractRpt.path("recordsWritten").asLong();
        long extractionErrors = extractRpt.path("extractionErrors").asLong();

        // Column names are read from contracts/artefacts.json, not spelled here: these
        // are the names the Python writers chose, and a rename there must reach this SQL
        // rather than surfacing as an empty count at 3am.
        String srcRunId = Artefacts.column("src", "run_id");
        String srcAccountKey = Artefacts.column("src", "account_key");
        String tgtRunId = Artefacts.column("target", "run_id");
        String tgtAccountKey = Artefacts.column("target", "account_key");

        long loadedToExtraction = bq.count(
                "SELECT COUNT(*) FROM `" + a.project + "." + a.dsExtraction
                        + ".account_src` WHERE " + srcRunId + " = " + q,
                qp);

        long distinctKeys = bq.count(
                "SELECT COUNT(DISTINCT " + srcAccountKey + ") FROM `" + a.project + "." + a.dsExtraction
                        + ".account_src` WHERE " + srcRunId + " = " + q,
                qp);

        long rejected = bq.count(
                "SELECT COUNT(*) FROM `" + a.project + "." + a.dsRecon
                        + ".reject_log` WHERE run_id = " + q,
                qp);

        // ── transformation / load reconciliation ─────────────────────────────────
        long curated = bq.count(
                "SELECT COUNT(*) FROM `" + a.project + "." + a.dsTransformation
                        + ".account_curated` WHERE run_id = " + q,
                qp);
        long enriched = bq.count(
                "SELECT COUNT(*) FROM `" + a.project + "." + a.dsTransformation
                        + ".account_enriched` WHERE run_id = " + q,
                qp);
        long targets = bq.count(
                "SELECT COUNT(*) FROM `" + a.project + "." + a.dsTransformation
                        + ".account_target` WHERE run_id = " + q,
                qp);
        long targetDistinct = bq.count(
                "SELECT COUNT(DISTINCT " + tgtAccountKey + ") FROM `" + a.project + "." + a.dsTransformation
                        + ".account_target` WHERE " + tgtRunId + " = " + q,
                qp);

        JsonNode loadRpt = readReport(store, a.reconBucket, a.runId, Artefacts.LANE_LOAD);
        long loadedDocuments = loadRpt.path("documentsRead").asLong();
        long loadErrors = loadRpt.path("errors").asLong();

        // Key-level: every TARGET key must trace back to an Extraction key, and vice
        // versa.
        long orphanTargets = bq.count(
                "SELECT COUNT(*) FROM `" + a.project + "." + a.dsTransformation + ".account_target` t "
                        + "LEFT JOIN `" + a.project + "." + a.dsExtraction + ".account_src` s "
                        + "ON t." + tgtAccountKey + " = s." + srcAccountKey
                        + " AND s." + srcRunId + " = " + q + " "
                        + "WHERE t." + tgtRunId + " = " + q + " AND s." + srcAccountKey + " IS NULL",
                qp);

        List<Map<String, String>> rejectBreakdown = bq.query(
                "SELECT stage, reason, COUNT(*) AS n FROM `" + a.project + "." + a.dsRecon
                        + ".reject_log` WHERE run_id = " + q + " GROUP BY stage, reason ORDER BY reason",
                qp);

        List<Map<String, String>> bandBreakdown = bq.query(
                "SELECT balance_band, COUNT(*) AS n FROM `" + a.project + "." + a.dsTransformation
                        + ".account_curated` WHERE run_id = " + q
                        + " GROUP BY balance_band ORDER BY balance_band",
                qp);

        // One read of the ledger row, rather than a query per column. The file processor
        // wrote it; recon reads it and then checks it against the register, instead of
        // asking BigQuery the same question twice and hoping the answers agree.
        List<Map<String, String>> ledgerRows = bq.query(
                "SELECT src_read, extraction_written, rejected, balanced FROM `"
                        + a.project + "." + a.dsRecon + ".run_ledger` WHERE run_id = " + q,
                qp);
        if (ledgerRows.isEmpty()) {
            throw new IllegalStateException(
                    "no run_ledger row for run " + a.runId + " — the file processor did not "
                            + "record its tallies, so there is nothing to reconcile against");
        }
        Map<String, String> ledger = ledgerRows.get(0);

        // The per-record register behind those tallies. If the two disagree, one of them
        // is wrong and reconciliation must say so rather than pick a side.
        List<Map<String, String>> lineageRows = bq.query(
                "SELECT door, COUNT(*) AS n FROM `" + a.project + "." + a.dsRecon
                        + ".record_lineage` WHERE run_id = " + q + " GROUP BY door",
                qp);
        long lineageRejected = 0;
        for (Map<String, String> row : lineageRows) {
            long n = Long.parseLong(row.getOrDefault("n", "0"));
            switch (row.getOrDefault("door", "")) {
                case "rejected" -> lineageRejected = n;
                default -> throw new IllegalStateException(
                        "record_lineage has an unknown door: " + row.get("door"));
            }
        }

        // ── target system reconciliation ───────────────────────────────────────────
        //
        // The balancing equation proves the Loader's books close. It does not prove Target
        // System kept what the Loader sent — a 201 is the Loader's success, not a row on
        // disk. The confirmation stream closes that gap: the mock publishes one event per
        // accepted write, keyed by account_key, and here we set-difference the TARGET keys
        // against the confirmation keys. Any TARGET row with no matching confirmation is
        // "sent but not confirmed" — the exact failure the plan wants recon to surface.
        //
        // The existing account_target queries above only COUNT; the join needs the actual
        // keys, so this reads them. The confirmation topic is read with a fresh consumer
        // group per run (recon-<runId>) so a re-run does not skip records a prior group
        // already committed past, and the read is bounded by the topic's end offsets so it
        // terminates even though the topic is never empty.
        List<String> targetKeys = targetKeys(bq, a, tgtRunId, tgtAccountKey, q, qp);
        boolean confirmationEnabled =
                a.confirmationBootstrap != null && !a.confirmationBootstrap.isBlank();
        Set<String> confirmationKeys = confirmationEnabled
                ? readConfirmations(a.confirmationBootstrap, a.confirmationTopic, a.runId)
                : Collections.emptySet();
        TargetSystemReconciliation targetSystem = matchConfirmations(targetKeys, confirmationKeys,
                confirmationEnabled);

        // ── the balancing equation, across the whole lane ────────────────────────
        //
        // srcRead is what the Extractor says it produced — deliberately the *upstream*
        // number, not one we derive ourselves, so a discrepancy anywhere in our own
        // lane
        // shows up as an imbalance rather than being defined away.
        Balance balance = Balance.from(ledger, srcExtracted, targets, rejected);

        boolean ledgerAgreesWithLineage = lineageRejected == rejected;

        // ── reports ──────────────────────────────────────────────────────────────
        ObjectNode report = MAPPER.createObjectNode();
        report.put("runId", a.runId);
        report.put("generatedAt", Instant.now().toString());

        ObjectNode source = report.putObject("sourceReconciliation");
        source.put("extractorReportedRecords", srcExtracted);
        source.put("extractionErrors", extractionErrors);
        source.put("loadedToExtractionDataset", loadedToExtraction);
        source.put("distinctAccountKeys", distinctKeys);
        source.put("duplicateKeysInExtraction", loadedToExtraction - distinctKeys);

        ObjectNode tl = report.putObject("transformationLoadReconciliation");
        tl.put("curatedRows", curated);
        tl.put("enrichedRows", enriched);
        tl.put("targetDocuments", targets);
        tl.put("targetDistinctKeys", targetDistinct);
        tl.put("orphanTargetKeys", orphanTargets);
        tl.put("loaderDocumentsRead", loadedDocuments);
        tl.put("loaderErrors", loadErrors);

        ObjectNode ts = report.putObject("targetSystemReconciliation");
        ts.put("enabled", targetSystem.enabled());
        ts.put("targetRows", targetSystem.targetRows());
        ts.put("confirmations", targetSystem.confirmations());
        ts.put("confirmedTargetRows", targetSystem.confirmedTargetRows());
        ts.put("unconfirmedTargetRows", targetSystem.unconfirmedTargetRows());
        ts.put("allTargetRowsConfirmed", targetSystem.allConfirmed());
        ArrayNode unconfirmed = ts.putArray("unconfirmedAccountKeys");
        for (String key : targetSystem.unconfirmedAccountKeys()) {
            unconfirmed.add(key);
        }

        ObjectNode eq = report.putObject("balancingEquation");
        eq.put("srcRead", balance.srcRead);
        eq.put("written", balance.written);
        eq.put("rejected", balance.rejected);
        eq.put("accounted", balance.accounted());
        eq.put("balances", balance.balances());
        eq.put("imbalance", balance.imbalance());

        // Aggregate claim vs per-record evidence, side by side and never silently merged.
        ObjectNode agreement = report.putObject("ledgerAgreement");
        agreement.put("rejectLogRejected", rejected);
        agreement.put("lineageRejected", lineageRejected);
        agreement.put("agrees", ledgerAgreesWithLineage);

        ArrayNode rejects = report.putArray("rejectsByReason");
        for (Map<String, String> row : rejectBreakdown) {
            ObjectNode node = rejects.addObject();
            node.put("stage", row.get("stage"));
            node.put("reason", row.get("reason"));
            node.put("count", Long.parseLong(row.getOrDefault("n", "0")));
        }

        ObjectNode migrability = report.putObject("migrability");
        migrability.put("candidates", balance.srcRead);
        migrability.put("migrated", balance.written);
        migrability.put("blockedByDataQuality", balance.rejected);
        migrability.put("migrabilityRate", rate(balance.written, balance.srcRead));
        ArrayNode bands = migrability.putArray("balanceBands");
        for (Map<String, String> row : bandBreakdown) {
            ObjectNode node = bands.addObject();
            node.put("band", row.get("balance_band"));
            node.put("count", Long.parseLong(row.getOrDefault("n", "0")));
        }

        String prefix = "reconciliation/" + a.runId + "/";
        byte[] json = write(report);
        store.put(a.reconBucket, prefix + "reconciliation-report.json", json);
        store.put(a.reconBucket, prefix + "reconciliation-report.html",
                Html.render(report).getBytes(StandardCharsets.UTF_8));

        System.out.println(new String(json, StandardCharsets.UTF_8));
        System.out.printf("recon: reports at gs://%s/%s%n", a.reconBucket, prefix);

        if (!balance.balances()) {
            System.err.printf(
                    "RECONCILIATION FAILED: equation does not close — srcRead=%d != "
                            + "migrated=%d + notMigrated=%d (rejected=%d) = %d, off by %d%n",
                    balance.srcRead, balance.written(), balance.notMigrated(),
                    balance.rejected, balance.accounted(), balance.imbalance());
            System.exit(1);
        }
        if (!ledgerAgreesWithLineage) {
            System.err.printf(
                    "RECONCILIATION FAILED: the ledger and the per-record register disagree — "
                            + "rejected %d vs %d. One of them is wrong, and 'which records did "
                            + "not migrate' has two answers.%n",
                    rejected, lineageRejected);
            System.exit(1);
        }
        if (orphanTargets > 0) {
            System.err.printf("RECONCILIATION FAILED: %d TARGET keys have no SRC row%n", orphanTargets);
            System.exit(1);
        }
        System.out.println("recon: balancing equation closes; key-level reconciliation clean.");

        // Step 4: an unconfirmed TARGET row is a run failure, not just a report entry —
        // "the Loader got a 201" is not "Target System persisted the row", and a
        // reconciliation that reports the gap and then exits 0 makes the gate a lie. The
        // negative path (mock's /__admin/suppress-next-confirmation) manufactures exactly
        // one unconfirmed row; this branch is what turns that into a non-zero exit and so
        // fails the Composer DAG. Disabled (no bootstrap) still skips cleanly, so a
        // no-Kafka run stays green: only an *enabled* stream that finds a gap fails.
        if (targetSystem.enabled()) {
            if (targetSystem.allConfirmed()) {
                System.out.printf("recon: target system confirmed all %d TARGET rows.%n",
                        targetSystem.confirmedTargetRows());
            } else {
                System.err.printf(
                        "RECONCILIATION FAILED: target system reconciliation GAP — %d of %d TARGET rows "
                                + "unconfirmed (confirmations seen: %d). Sent but not persisted.%n",
                        targetSystem.unconfirmedTargetRows(), targetSystem.targetRows(),
                        targetSystem.confirmations());
                System.exit(1);
            }
        } else {
            System.out.println("recon: target system reconciliation skipped (no confirmation bootstrap).");
        }
    }

    /**
     * Result of the Target System confirmation join, exposed as a record so the match
     * logic is testable without Kafka or BigQuery — {@link ReconciliationMatcherTest}
     * exercises {@link #matchConfirmations(List, Set, boolean)} directly.
     */
    public record TargetSystemReconciliation(
            boolean enabled,
            long targetRows,
            long confirmations,
            List<String> unconfirmedAccountKeys) {

        /** TARGET rows that have a matching confirmation key. */
        long confirmedTargetRows() {
            return targetRows - unconfirmedAccountKeys.size();
        }

        /** TARGET rows with no matching confirmation — the "sent but not confirmed" gap. */
        long unconfirmedTargetRows() {
            return unconfirmedAccountKeys.size();
        }

        boolean allConfirmed() {
            return enabled && unconfirmedAccountKeys.isEmpty();
        }
    }

    /**
     * Set-difference join on account_key: every TARGET key with no matching confirmation
     * is unconfirmed. When disabled (no bootstrap), returns an empty gap with
     * {@code enabled=false} so the report and acceptance criterion 9 can skip cleanly
     * rather than misreading "no stream configured" as "zero confirmations".
     */
    static TargetSystemReconciliation matchConfirmations(
            List<String> targetKeys, Set<String> confirmationKeys, boolean enabled) {
        if (!enabled) {
            return new TargetSystemReconciliation(false, targetKeys.size(), 0, List.of());
        }
        List<String> unconfirmed = new ArrayList<>();
        // A TARGET key that appears more than once is itself a defect the key-level check
        // above already fails the run on, so the set-difference is well-defined over
        // distinct keys. Iterate the target list (not the set) so the unconfirmed list is
        // stable across runs for the same input — easier to diff two reports at 2am.
        Set<String> seen = new HashSet<>();
        for (String key : targetKeys) {
            if (!seen.add(key)) {
                continue; // a duplicate TARGET key; orphanTargetKeys/the key check handles it
            }
            if (!confirmationKeys.contains(key)) {
                unconfirmed.add(key);
            }
        }
        return new TargetSystemReconciliation(true, targetKeys.size(), confirmationKeys.size(),
                Collections.unmodifiableList(unconfirmed));
    }

    /**
     * Reads the account_key column of account_target for this run — the keys the set
     * difference joins on. Run id is parameterised as everywhere else; identifiers are
     * validated at {@link Args#parse}.
     */
    private static List<String> targetKeys(BigQueryRest bq, Args a,
                                           String tgtRunId, String tgtAccountKey,
                                           String q, Map<String, String> qp) {
        List<Map<String, String>> rows = bq.query(
                "SELECT " + tgtAccountKey + " AS k FROM `" + a.project + "." + a.dsTransformation
                        + ".account_target` WHERE " + tgtRunId + " = " + q,
                qp);
        List<String> keys = new ArrayList<>(rows.size());
        for (Map<String, String> row : rows) {
            String key = row.get("k");
            if (key != null && !key.isBlank()) {
                keys.add(key);
            }
        }
        return keys;
    }

    /**
     * Reads confirmation events for {@code runId} from the topic with a fresh per-run
     * consumer group ({@code recon-<runId>}), so a re-run does not skip records a prior
     * group committed past. The read is bounded by the topic's end offsets: assign all
     * partitions, seek to the beginning, poll until every partition has reached its end
     * offset (or a short sanity timeout fires), and keep only the account keys whose
     * event belongs to this run — a topic in steady state is never empty, so the bound is
     * what makes the read terminate.
     */
    private static Set<String> readConfirmations(String bootstrap, String topic, String runId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Fresh group per run → always read from the earliest offset, never a committed
        // position from a previous run. The run id is already identifier-validated.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "recon-" + runId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The confirmation stream is small and short-lived; keep the polls cheap.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        // Same OAUTHBEARER wiring as the mock producer: GCP Managed Kafka is SASL_SSL with a
        // Google access token, locally redpanda is PLAINTEXT. KAFKA_SECURITY_PROTOCOL gates
        // it so the local stack (PLAINTEXT, the default) is unchanged. The callback handler
        // reuses the GcpToken token source; the JAAS module is required even with a custom
        // handler (see apps/common GcpTokenOauthCallbackHandler).
        if (!"PLAINTEXT".equals(System.getenv().getOrDefault("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "OAUTHBEARER");
            props.put("sasl.login.callback.handler.class",
                    "ro.mig.common.GcpTokenOauthCallbackHandler");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        }

        Set<String> keys = new HashSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = listPartitions(consumer, topic);
            if (partitions.isEmpty()) {
                System.err.printf("recon: confirmation topic %s has no partitions; "
                        + "treating confirmations as empty.%n", topic);
                return keys;
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            // Safety bound: the read must terminate even if a producer is mid-publish. The
            // end-offset check is the real terminator; this is the backstop for a broker
            // that stops advancing them.
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (!allReachedEnd(consumer, endOffsets) && System.nanoTime() < deadline) {
                Duration pollTimeout = Duration.ofMillis(500);
                for (ConsumerRecord<String, String> record : consumer.poll(pollTimeout)) {
                    if (matchesRun(record.value(), runId)) {
                        String key = record.key();
                        if (key != null && !key.isBlank()) {
                            keys.add(key);
                        }
                    }
                }
                // endOffsets is a snapshot taken once; a producer publishing during the
                // read would make new records past it, which is fine — those belong to a
                // later run and the runId filter drops them anyway.
            }
        }
        return keys;
    }

    private static boolean matchesRun(String json, String runId) {
        if (json == null || json.isBlank()) {
            return false;
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            return runId.equals(node.path("runId").asText(""));
        } catch (Exception e) {
            // A malformed confirmation is not a reason to fail reconciliation — it is a
            // reason to report the row it would have confirmed as unconfirmed.
            System.err.printf("recon: could not parse confirmation event, ignoring: %s%n",
                    e.getMessage());
            return false;
        }
    }

    private static List<TopicPartition> listPartitions(KafkaConsumer<String, String> consumer,
                                                       String topic) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (org.apache.kafka.common.PartitionInfo info : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(topic, info.partition()));
        }
        return partitions;
    }

    private static boolean allReachedEnd(KafkaConsumer<String, String> consumer,
                                          Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
            long position = consumer.position(e.getKey());
            if (position < e.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * The balancing equation: two doors, migrated and not-migrated.
     *
     * <p>
     * Field names stay {@code written}/{@code rejected} because they are the
     * published keys of {@code reconciliation-report.json} and the
     * {@code run_ledger} column names — the two-door framing is a presentation
     * layer over them, deliberately not a rename, so archived evidence reports
     * stay comparable with new ones.
     *
     * <p>
     * Each number is read from where it was actually recorded, not re-derived.
     */
    private record Balance(long srcRead, long written, long rejected) {

        static Balance from(Map<String, String> ledger, long srcRead, long written, long rejected) {
            // rejected is passed in from the reject_log count, not read off the ledger
            // row, so the equation still checks an independently observed number.
            // srcRead comes from the extractor's .RPT and written from the target
            // table, so every term originates outside this class.
            return new Balance(srcRead, written, rejected);
        }

        /** Door 2 of 2. */
        long notMigrated() {
            return rejected;
        }

        long accounted() {
            // Identical either way: written (migrated) + notMigrated().
            return written + rejected;
        }

        boolean balances() {
            return srcRead == accounted();
        }

        long imbalance() {
            return srcRead - accounted();
        }
    }

    private static double rate(long numerator, long denominator) {
        return denominator == 0 ? 0.0 : Math.round((numerator * 10000.0) / denominator) / 100.0;
    }

    private static JsonNode readReport(ObjectStore store, String bucket, String runId, String lane) {
        String name = Artefacts.prefix(lane, runId) + Artefacts.rpt("ACCOUNT");
        try {
            return MAPPER.readTree(store.get(bucket, name));
        } catch (Exception e) {
            throw new IllegalStateException("could not read " + lane + " .RPT at gs://"
                    + bucket + "/" + name, e);
        }
    }

    private static byte[] write(ObjectNode node) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(node);
        } catch (Exception e) {
            throw new IllegalStateException("could not render the reconciliation report", e);
        }
    }

    private static final class Args {
        String gcsHost = "http://localhost:4443";
        String bqHost = "http://localhost:9050";
        String project = "mig-local";
        String landingBucket = "mig-landing";
        String reconBucket = "mig-recon";
        String dsExtraction = "bq_extraction";
        String dsTransformation = "bq_transformation";
        String dsRecon = "bq_recon";
        String runId = "";
        String confirmationBootstrap = "";
        String confirmationTopic = "target-system-confirmations";

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length - 1; i += 2) {
                String v = argv[i + 1];
                switch (argv[i]) {
                    case "--gcs-host" -> a.gcsHost = v;
                    case "--bq-host" -> a.bqHost = v;
                    case "--project" -> a.project = v;
                    case "--landing-bucket" -> a.landingBucket = v;
                    case "--recon-bucket" -> a.reconBucket = v;
                    case "--ds-extraction" -> a.dsExtraction = v;
                    case "--ds-transformation" -> a.dsTransformation = v;
                    case "--ds-recon" -> a.dsRecon = v;
                    case "--run-id" -> a.runId = v;
                    case "--confirmation-bootstrap" -> a.confirmationBootstrap = v;
                    case "--confirmation-topic" -> a.confirmationTopic = v;
                    default -> throw new IllegalArgumentException("unknown option: " + argv[i]);
                }
            }
            if (a.runId.isBlank()) {
                throw new IllegalArgumentException("--run-id is required");
            }
            // Project and dataset names are interpolated into SQL because parameters
            // cannot bind identifiers. Validate them so that remains safe. The run id is
            // parameterised and needs no such restriction, but it is checked anyway —
            // it also names GCS object paths.
            requireIdentifier("--project", a.project);
            requireIdentifier("--ds-extraction", a.dsExtraction);
            requireIdentifier("--ds-transformation", a.dsTransformation);
            requireIdentifier("--ds-recon", a.dsRecon);
            requireIdentifier("--run-id", a.runId);
            return a;
        }

        private static final java.util.regex.Pattern SAFE_IDENTIFIER = java.util.regex.Pattern
                .compile("^[A-Za-z0-9_.-]+$");

        private static void requireIdentifier(String option, String value) {
            if (!SAFE_IDENTIFIER.matcher(value).matches()) {
                throw new IllegalArgumentException(
                        option + " must match " + SAFE_IDENTIFIER.pattern() + " but was: " + value);
            }
        }
    }
}
