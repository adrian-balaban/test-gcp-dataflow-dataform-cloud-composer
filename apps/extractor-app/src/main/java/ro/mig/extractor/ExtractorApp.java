package ro.mig.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ro.mig.common.Archives;
import ro.mig.common.Artefacts;
import ro.mig.common.Checksums;
import ro.mig.common.HttpObjectStore;
import ro.mig.common.ObjectStore;
import ro.mig.common.Pgp;
import ro.mig.common.RunContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The Extractor App from architecture diagram's Extraction lane.
 *
 * <p>
 * Owned by the other team in reality; here it is a faithful mock that honours
 * the stated
 * contract so the rest of the chain can run without waiting for real data. It
 * reads a
 * table2table-style dump and produces the five artefacts, then
 * archives &rarr; compresses &rarr; PGP-encrypts them and lands the bundle in
 * File Storage.
 *
 * <p>
 * The {@code .FLG} semaphore is uploaded <em>last</em>, and only after the
 * bundle is durably
 * stored: that ordering is the entire contract the downstream Dataflow file
 * processor waits on.
 */
public final class ExtractorApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        Args a = Args.parse(args);
        RunContext run = new RunContext(a.runId);

        ObjectStore store = HttpObjectStore.fromEnv(a.gcsHost);
        store.createBucket(a.bucket);

        List<String> lines = readSource(a.input);
        // The pipe-delimited CSV extract always carries a header row as its first line
        // (contracts/README.md), which is transport, not a record — the Extractor is
        // otherwise deliberately format-agnostic (it never parses fields), but "how many
        // records did I read" is a claim the .RPT makes about actual records, and
        // counting the header would report one too many, the same way ReadBundleFn
        // downstream must not count it into src_read.
        if (!lines.isEmpty()) {
            lines = lines.subList(1, lines.size());
        }
        Split split = split(lines, a.splitSize, a.injectExtractErrors);

        Map<String, byte[]> bundleFiles = new LinkedHashMap<>();
        List<Checksums.Entry> checksums = new ArrayList<>();

        for (int i = 0; i < split.dataFiles.size(); i++) {
            String name = Artefacts.dat(a.record, i + 1);
            byte[] payload = Checksums.utf8(String.join("\n", split.dataFiles.get(i)) + "\n");
            bundleFiles.put(name, payload);
            checksums.add(new Checksums.Entry(
                    Checksums.sha256(payload), Checksums.countRecords(payload), name));
        }

        byte[] errPayload = Checksums.utf8(renderErrors(split.errors));
        bundleFiles.put(Artefacts.err(a.record), errPayload);

        byte[] chsPayload = Checksums.utf8(Checksums.render(run, checksums));
        bundleFiles.put(Artefacts.chs(a.record), chsPayload);

        byte[] rptPayload = renderReport(a, run, split, checksums);
        bundleFiles.put(Artefacts.rpt(a.record), rptPayload);

        // archiving -> compression -> PGP encryption
        byte[] archive = Archives.tarGz(bundleFiles);
        byte[] encrypted = new Pgp(Path.of(a.gnupgHome)).encrypt(archive, a.recipient);

        String prefix = Artefacts.prefix(Artefacts.LANE_EXTRACTION, run.runId());
        store.put(a.bucket, prefix + Artefacts.bundle(a.record), encrypted);

        // The semaphore goes up last — everything it vouches for is already durable.
        store.put(a.bucket, prefix + Artefacts.flg(a.record), renderFlag(run, bundleFiles.keySet()));

        System.out.printf(
                "extractor: run=%s records=%d dat_files=%d extraction_errors=%d -> gs://%s/%s%n",
                run.runId(), split.totalRecords, split.dataFiles.size(),
                split.errors.size(), a.bucket, prefix);
    }

    // ── source ───────────────────────────────────────────────────────────────────

    private static List<String> readSource(String input) {
        try {
            return Files.readAllLines(Path.of(input), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read source extract: " + input, e);
        }
    }

    /**
     * One extraction-level failure: a row the Extractor itself could not read out
     * of Db2.
     */
    private record ExtractError(long lineNumber, String reason, String raw) {
    }

    private record Split(List<List<String>> dataFiles, List<ExtractError> errors, long totalRecords) {
    }

    /**
     * Split the extract into sequenced {@code .DAT} files, diverting unreadable
     * rows to
     * {@code .ERR}.
     *
     * <p>
     * Note the boundary: only rows that are unreadable <em>as rows</em> are
     * extraction errors.
     * Records that are well-formed but semantically invalid stay in the
     * {@code .DAT} file and are
     * the transformation engine's business — that is what keeps "failed to extract"
     * and "failed
     * to transform" from being conflated.
     */
    private static Split split(List<String> lines, int splitSize, int injectErrors) {
        List<List<String>> files = new ArrayList<>();
        List<ExtractError> errors = new ArrayList<>();
        List<String> current = new ArrayList<>();
        long total = 0;
        int injected = 0;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);

            if (line.isBlank()) {
                continue;
            }
            if (line.indexOf('\0') >= 0) {
                errors.add(new ExtractError(i + 1L, "NUL_BYTE_IN_RECORD", truncate(line)));
                continue;
            }
            if (injected < injectErrors) {
                // Simulated Db2 read failure, so the .ERR path is exercised on demand.
                errors.add(new ExtractError(i + 1L, "SIMULATED_DB2_READ_FAILURE", truncate(line)));
                injected++;
                continue;
            }

            current.add(line);
            total++;
            if (current.size() >= splitSize) {
                files.add(current);
                current = new ArrayList<>();
            }
        }
        if (!current.isEmpty() || files.isEmpty()) {
            files.add(current);
        }
        return new Split(files, errors, total);
    }

    private static String truncate(String line) {
        return line.length() <= 120 ? line : line.substring(0, 120) + "...";
    }

    // ── artefact rendering ───────────────────────────────────────────────────────

    private static String renderErrors(List<ExtractError> errors) {
        StringBuilder sb = new StringBuilder("# MIG 000001-1 extraction errors\n");
        sb.append("# line\treason\traw\n");
        for (ExtractError e : errors) {
            sb.append(e.lineNumber()).append('\t').append(e.reason()).append('\t').append(e.raw()).append('\n');
        }
        return sb.toString();
    }

    private static byte[] renderReport(
            Args a, RunContext run, Split split, List<Checksums.Entry> checksums) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("lane", Artefacts.LANE_EXTRACTION);
        root.put("record", a.record);
        root.put("runId", run.runId());
        root.put("generatedAt", Instant.now().toString());
        root.put("sourceRecordsRead", split.totalRecords + split.errors.size());
        root.put("recordsWritten", split.totalRecords);
        root.put("extractionErrors", split.errors.size());

        ArrayNode files = root.putArray("datFiles");
        for (Checksums.Entry entry : checksums) {
            ObjectNode node = files.addObject();
            node.put("name", entry.fileName());
            node.put("records", entry.records());
            node.put("sha256", entry.sha256());
        }
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
        } catch (Exception e) {
            throw new IllegalStateException("could not render .RPT", e);
        }
    }

    private static byte[] renderFlag(RunContext run, Iterable<String> bundled) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("runId", run.runId());
        root.put("completedAt", Instant.now().toString());
        ArrayNode files = root.putArray("bundledArtefacts");
        bundled.forEach(files::add);
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
        } catch (Exception e) {
            throw new IllegalStateException("could not render .FLG", e);
        }
    }

    // ── arguments ────────────────────────────────────────────────────────────────

    private static final class Args {
        String input = "local/data/mainframe/ACCOUNT.src";
        String record = "ACCOUNT";
        String bucket = "mig-landing";
        String gcsHost = "http://localhost:4443";
        String runId = RunContext.newRunId();
        int splitSize = 5000;
        int injectExtractErrors = 0;
        String recipient = "mig-prototype@example.invalid";
        String gnupgHome = "local/keys";

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length - 1; i += 2) {
                String value = argv[i + 1];
                switch (argv[i]) {
                    case "--input" -> a.input = value;
                    case "--record" -> a.record = value;
                    case "--bucket" -> a.bucket = value;
                    case "--gcs-host" -> a.gcsHost = value;
                    case "--run-id" -> a.runId = value;
                    case "--split" -> a.splitSize = Integer.parseInt(value);
                    case "--inject-extract-errors" -> a.injectExtractErrors = Integer.parseInt(value);
                    case "--recipient" -> a.recipient = value;
                    case "--gnupg-home" -> a.gnupgHome = value;
                    default -> throw new IllegalArgumentException("unknown option: " + argv[i]);
                }
            }
            return a;
        }
    }
}
