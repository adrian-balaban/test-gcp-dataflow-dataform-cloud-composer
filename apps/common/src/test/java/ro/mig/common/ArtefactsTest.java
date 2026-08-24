package ro.mig.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Java half of the cross-language contract.
 *
 * <p>
 * The Python half lives in {@code tests/test_engine.py}. Both assert the same thing from
 * opposite sides: that artefact names are <em>derived from</em>
 * {@code contracts/artefacts.json} rather than restated in code. Before the manifest
 * existed, a rename in Python reached Java only via a comment, and the drift surfaced as a
 * failed run rather than a failed build — which is precisely what these tests convert it
 * back into.
 */
class ArtefactsTest {

    /** The manifest read straight off disk — the oracle, independent of Artefacts. */
    private static JsonNode manifest() throws Exception {
        Path source = Path.of("..", "..", "contracts", "artefacts.json");
        assertTrue(Files.isRegularFile(source), "contracts/artefacts.json not found at " + source);
        return new ObjectMapper().readTree(Files.readString(source));
    }

    @Test
    void namesAreDerivedFromTheManifestNotSpelledInCode() throws Exception {
        JsonNode artefacts = manifest().path("artefacts");

        assertEquals(artefacts.path("chs").asText().replace("{record}", "ACCOUNT"),
                Artefacts.chs("ACCOUNT"));
        assertEquals(artefacts.path("err").asText().replace("{record}", "ACCOUNT"),
                Artefacts.err("ACCOUNT"));
        assertEquals(artefacts.path("rpt").asText().replace("{record}", "ACCOUNT"),
                Artefacts.rpt("ACCOUNT"));
        assertEquals(artefacts.path("flg").asText().replace("{record}", "ACCOUNT"),
                Artefacts.flg("ACCOUNT"));
        assertEquals(artefacts.path("bundle").asText().replace("{record}", "ACCOUNT"),
                Artefacts.bundle("ACCOUNT"));
    }

    @Test
    void datIsZeroPaddedToTheWidthTheManifestDeclares() throws Exception {
        int width = manifest().path("dat_sequence_width").asInt();
        String expected = manifest().path("artefacts").path("dat").asText()
                .replace("{record}", "ACCOUNT")
                .replace("{sequence}", String.format("%0" + width + "d", 7));

        // Both languages pad from the same number, so a split extract written by the
        // extractor is found by the file processor rather than missed by one digit.
        assertEquals(expected, Artefacts.dat("ACCOUNT", 7));
        assertTrue(Artefacts.isDat(Artefacts.dat("ACCOUNT", 7)));
    }

    @Test
    void lanePrefixMatchesTheManifest() throws Exception {
        String template = manifest().path("prefix").asText();
        String expected = template
                .replace("{lane}", manifest().path("lanes").path("extraction").asText())
                .replace("{run_id}", "initial-1234-ab12");

        assertEquals(expected, Artefacts.prefix(Artefacts.LANE_EXTRACTION, "initial-1234-ab12"));
        assertEquals("extraction", Artefacts.LANE_EXTRACTION);
        assertEquals("load", Artefacts.LANE_LOAD);
    }

    @Test
    void columnLookupsResolveTheNamesTheSqlDependsOn() throws Exception {
        JsonNode columns = manifest().path("columns");

        // recon-service builds its SQL from these; a wrong answer here is a query that
        // returns zero rows rather than one that fails, which is the dangerous shape.
        assertEquals(columns.path("src").path("run_id").asText(), Artefacts.column("src", "run_id"));
        assertEquals(columns.path("src").path("account_key").asText(),
                Artefacts.column("src", "account_key"));
        assertEquals(columns.path("target").path("account_key").asText(),
                Artefacts.column("target", "account_key"));
    }

    @Test
    void anUnknownKeyFailsLoudlyRatherThanReturningNull() {
        // A missing entry must not degrade into "null" spliced into an object name or a
        // SQL statement, which would fail somewhere far from the cause.
        assertThrows(IllegalStateException.class, () -> Artefacts.column("src", "no_such_column"));
        assertThrows(IllegalStateException.class, () -> Artefacts.column("no_such_table", "run_id"));
    }
}
