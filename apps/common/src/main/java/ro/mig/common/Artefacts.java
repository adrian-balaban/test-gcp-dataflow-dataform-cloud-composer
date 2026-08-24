package ro.mig.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.IOException;
import java.util.Map;

/**
 * The five artefact types from the architecture diagram, and the object naming
 * convention shared by every component that reads or writes them.
 *
 * <pre>
 *   .DAT  flat file, source data (split files if needed)
 *   .CHS  checksums for reconciliation
 *   .ERR  errors
 *   .RPT  report files (#records etc)
 *   .FLG  flag file — the semaphore that all files were generated
 * </pre>
 *
 * <p>
 * Both outer lanes produce the same five types: extraction on the way in, load
 * on the way out. The {@code lane} prefix keeps them apart in the bucket.
 *
 * <p>
 * <b>The convention is not written here.</b> It is loaded from
 * {@code contracts/artefacts.json}, the same file
 * {@code pipelines/common/artefacts.py} reads — Maven copies it onto this
 * module's classpath at build time. Until that manifest existed, the convention
 * was declared twice, once per language, and kept in step by a comment in each;
 * a rename on the Python side stayed invisible here until a run failed at
 * integration.
 */
public final class Artefacts {

    private static final JsonNode MANIFEST = load();

    public static final String LANE_EXTRACTION = lane("extraction");
    public static final String LANE_LOAD = lane("load");

    private Artefacts() {
    }

    private static JsonNode load() {
        try (InputStream in = Artefacts.class.getResourceAsStream("/artefacts.json")) {
            if (in == null) {
                // A hardcoded fallback would silently reintroduce the drift this class
                // exists to prevent, so an absent manifest is fatal rather than papered
                // over: it means the jar was built without contracts/artefacts.json.
                throw new IllegalStateException(
                        "artefacts.json is not on the classpath — the jar was built without "
                                + "contracts/artefacts.json; check apps/common/pom.xml <resources>");
            }
            return new ObjectMapper().readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the artefact manifest", e);
        }
    }

    private static String template(String key) {
        JsonNode node = MANIFEST.path("artefacts").path(key);
        if (node.isMissingNode()) {
            throw new IllegalStateException("artefact manifest declares no '" + key + "'");
        }
        return node.asText();
    }

    private static String lane(String key) {
        JsonNode node = MANIFEST.path("lanes").path(key);
        if (node.isMissingNode()) {
            throw new IllegalStateException("artefact manifest declares no lane '" + key + "'");
        }
        return node.asText();
    }

    /** Substitutes {@code {record}} / {@code {sequence}} / {@code {lane}} / {@code {run_id}}. */
    private static String fill(String template, Map<String, String> values) {
        String out = template;
        for (Map.Entry<String, String> e : values.entrySet()) {
            out = out.replace("{" + e.getKey() + "}", e.getValue());
        }
        return out;
    }

    /** e.g. {@code extraction/initial-1234-ab12/} */
    public static String prefix(String lane, String runId) {
        return fill(MANIFEST.path("prefix").asText(), Map.of("lane", lane, "run_id", runId));
    }

    /**
     * Split data files are sequenced, so a large extract can be produced in parts.
     * The pad width comes from the manifest, so both languages zero-pad alike.
     */
    public static String dat(String record, int sequence) {
        int width = MANIFEST.path("dat_sequence_width").asInt();
        return fill(template("dat"), Map.of(
                "record", record,
                "sequence", String.format("%0" + width + "d", sequence)));
    }

    public static String chs(String record) {
        return fill(template("chs"), Map.of("record", record));
    }

    public static String err(String record) {
        return fill(template("err"), Map.of("record", record));
    }

    public static String rpt(String record) {
        return fill(template("rpt"), Map.of("record", record));
    }

    /**
     * The semaphore. Written <em>last</em> and outside the encrypted bundle, so a
     * consumer can detect "the extract is complete" without holding a decryption key.
     */
    public static String flg(String record) {
        return fill(template("flg"), Map.of("record", record));
    }

    /**
     * Archived, compressed and PGP-encrypted transport unit holding DAT/CHS/ERR/RPT.
     */
    public static String bundle(String record) {
        return fill(template("bundle"), Map.of("record", record));
    }

    public static boolean isDat(String name) {
        return name.endsWith(".DAT");
    }

    /**
     * Physical BigQuery column name for a logical field — {@code table} is
     * {@code src} or {@code target}. recon-service reads both sets by name, which is
     * why the mapping belongs in the manifest and not in either language's source.
     */
    public static String column(String table, String logical) {
        JsonNode node = MANIFEST.path("columns").path(table).path(logical);
        if (node.isMissingNode()) {
            throw new IllegalStateException(
                    "artefact manifest declares no column '" + table + "." + logical + "'");
        }
        return node.asText();
    }
}
