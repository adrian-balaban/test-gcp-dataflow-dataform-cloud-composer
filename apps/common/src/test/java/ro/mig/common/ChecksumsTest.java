package ro.mig.common;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The `.CHS` round trip — the artefact reconciliation actually leans on.
 *
 * <p>
 * Acceptance criterion 9 re-verifies checksums against the live stack, but only for data
 * the harness produced. These cover the cases a real extract will eventually produce and a
 * synthetic one never does: a truncated file, a file nobody listed, a manifest entry with
 * no payload.
 */
class ChecksumsTest {

    private static final RunContext RUN =
            new RunContext("initial-1234");

    @Test
    void renderAndParseAreInverses() {
        byte[] payload = Checksums.utf8("one\ntwo\nthree\n");
        Checksums.Entry entry = new Checksums.Entry(
                Checksums.sha256(payload), Checksums.countRecords(payload), "ACCOUNT.001.DAT");

        Map<String, Checksums.Entry> parsed = Checksums.parse(Checksums.render(RUN, List.of(entry)));

        assertEquals(1, parsed.size());
        assertEquals(entry, parsed.get("ACCOUNT.001.DAT"));
    }

    @Test
    void aFinalLineWithoutANewlineStillCounts() {
        // Mainframe extracts do not reliably end with a newline; miscounting the last
        // record would fail the balancing equation by exactly one, for every file.
        assertEquals(3, Checksums.countRecords(Checksums.utf8("one\ntwo\nthree")));
        assertEquals(3, Checksums.countRecords(Checksums.utf8("one\ntwo\nthree\n")));
    }

    @Test
    void verifyReportsEveryKindOfDiscrepancy() {
        byte[] good = Checksums.utf8("a\nb\n");
        Map<String, Checksums.Entry> manifest = Map.of(
                "GOOD.DAT", new Checksums.Entry(Checksums.sha256(good), 2, "GOOD.DAT"),
                "TAMPERED.DAT", new Checksums.Entry(Checksums.sha256(good), 2, "TAMPERED.DAT"),
                "ABSENT.DAT", new Checksums.Entry(Checksums.sha256(good), 2, "ABSENT.DAT"));

        List<String> problems = Checksums.verify(manifest, Map.of(
                "GOOD.DAT", good,
                "TAMPERED.DAT", Checksums.utf8("a\nB\n"),
                "UNLISTED.DAT", good));

        assertEquals(3, problems.size(), "expected exactly one problem per discrepancy: " + problems);
        assertTrue(problems.stream().anyMatch(s -> s.startsWith("checksum mismatch for TAMPERED.DAT")));
        assertTrue(problems.stream().anyMatch(s -> s.startsWith("missing file listed in .CHS: ABSENT.DAT")));
        assertTrue(problems.stream().anyMatch(s -> s.startsWith("file not listed in .CHS: UNLISTED.DAT")));
    }

    @Test
    void anIntactExtractHasNoProblems() {
        byte[] payload = Checksums.utf8("x\ny\nz\n");
        Map<String, Checksums.Entry> manifest = Map.of("ACCOUNT.001.DAT",
                new Checksums.Entry(Checksums.sha256(payload), 3, "ACCOUNT.001.DAT"));

        assertTrue(Checksums.verify(manifest, Map.of("ACCOUNT.001.DAT", payload)).isEmpty());
    }
}
