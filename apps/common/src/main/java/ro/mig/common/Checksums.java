package ro.mig.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code .CHS} artefact — checksums for reconciliation, on both the extraction and the
 * load side (architecture diagram, both outer lanes).
 *
 * <p>Each line carries the SHA-256 of a data file plus its record count, so reconciliation can
 * detect both corruption (digest) and truncation (count) without re-reading the payload.
 */
public final class Checksums {

    private Checksums() {
    }

    /** One checksum line. */
    public record Entry(String sha256, long records, String fileName) {
    }

    public static String sha256(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static long countRecords(byte[] content) {
        if (content.length == 0) {
            return 0;
        }
        long lines = 0;
        for (byte b : content) {
            if (b == '\n') {
                lines++;
            }
        }
        // Tolerate a final line with no trailing newline.
        if (content[content.length - 1] != '\n') {
            lines++;
        }
        return lines;
    }

    public static String render(RunContext run, List<Entry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append("# MIG 000001-1 checksum manifest\n");
        sb.append("# run_id=").append(run.runId()).append('\n');
        sb.append("# sha256  records  file\n");
        for (Entry e : entries) {
            sb.append(e.sha256()).append("  ").append(e.records()).append("  ").append(e.fileName()).append('\n');
        }
        return sb.toString();
    }

    public static Map<String, Entry> parse(String text) {
        Map<String, Entry> out = new LinkedHashMap<>();
        for (String line : text.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            if (parts.length < 3) {
                throw new IllegalArgumentException("malformed .CHS line: " + line);
            }
            out.put(parts[2], new Entry(parts[0], Long.parseLong(parts[1]), parts[2]));
        }
        return out;
    }

    /**
     * Verify payloads against a parsed manifest.
     *
     * @return the list of discrepancies; empty means the extract is intact.
     */
    public static List<String> verify(Map<String, Entry> manifest, Map<String, byte[]> payloads) {
        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, Entry> expected : manifest.entrySet()) {
            byte[] payload = payloads.get(expected.getKey());
            if (payload == null) {
                problems.add("missing file listed in .CHS: " + expected.getKey());
                continue;
            }
            String actual = sha256(payload);
            if (!actual.equals(expected.getValue().sha256())) {
                problems.add("checksum mismatch for " + expected.getKey()
                        + ": expected " + expected.getValue().sha256() + " got " + actual);
            }
            long records = countRecords(payload);
            if (records != expected.getValue().records()) {
                problems.add("record count mismatch for " + expected.getKey()
                        + ": expected " + expected.getValue().records() + " got " + records);
            }
        }
        for (String present : payloads.keySet()) {
            if (!manifest.containsKey(present)) {
                problems.add("file not listed in .CHS: " + present);
            }
        }
        return problems;
    }

    public static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
