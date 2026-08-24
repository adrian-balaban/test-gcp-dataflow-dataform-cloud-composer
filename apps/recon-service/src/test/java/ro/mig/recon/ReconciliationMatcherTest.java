package ro.mig.recon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import ro.mig.recon.ReconService.TargetSystemReconciliation;

/**
 * Exercises the set-difference join in {@link ReconService#matchConfirmations} — the one
 * piece of the Target System reconciliation that has to be exactly right before the Kafka
 * and BigQuery plumbing are worth wiring. Pure logic, no broker, no BigQuery: each case
 * pins a behaviour the plan (docs/PLAN-CHANGES-22082026.md) depends on.
 */
class ReconciliationMatcherTest {

    @Test
    void allTargetRowsConfirmedWhenEveryKeyHasAConfirmation() {
        TargetSystemReconciliation r = ReconService.matchConfirmations(
                List.of("k1", "k2", "k3"), Set.of("k1", "k2", "k3", "extra"), true);

        assertTrue(r.enabled());
        assertEquals(3, r.targetRows());
        assertEquals(4, r.confirmations());
        assertEquals(3, r.confirmedTargetRows());
        assertEquals(0, r.unconfirmedTargetRows());
        assertTrue(r.allConfirmed());
        assertTrue(r.unconfirmedAccountKeys().isEmpty());
    }

    @Test
    void namesTheUnconfirmedRowsWhenSomeTargetKeysHaveNoConfirmation() {
        TargetSystemReconciliation r = ReconService.matchConfirmations(
                List.of("k1", "k2", "k3"), Set.of("k2"), true);

        assertTrue(r.enabled());
        assertEquals(3, r.targetRows());
        assertEquals(1, r.confirmations());
        assertEquals(1, r.confirmedTargetRows());
        assertEquals(2, r.unconfirmedTargetRows());
        assertFalse(r.allConfirmed());
        // Stable order, matching the input order — diffing two reports at 2am works.
        assertEquals(List.of("k1", "k3"), r.unconfirmedAccountKeys());
    }

    @Test
    void zeroConfirmationsMeansEveryTargetRowUnconfirmed() {
        TargetSystemReconciliation r = ReconService.matchConfirmations(
                List.of("k1", "k2"), Set.of(), true);

        assertTrue(r.enabled());
        assertEquals(2, r.unconfirmedTargetRows());
        assertEquals(0, r.confirmedTargetRows());
        assertFalse(r.allConfirmed());
        assertEquals(List.of("k1", "k2"), r.unconfirmedAccountKeys());
    }

    @Test
    void disabledReportsEnabledFalseAndEmptyGapSoAcceptanceCanSkip() {
        TargetSystemReconciliation r = ReconService.matchConfirmations(
                List.of("k1", "k2"), Set.of(), false);

        // The no-Kafka path: "no stream configured" must not read as "zero confirmations".
        assertFalse(r.enabled());
        assertEquals(2, r.targetRows());
        assertEquals(0, r.confirmations());
        assertEquals(0, r.unconfirmedTargetRows());
        // allConfirmed is false when disabled, which is what tells acceptance criterion 9
        // to skip rather than assert a clean confirmation result.
        assertFalse(r.allConfirmed());
        assertTrue(r.unconfirmedAccountKeys().isEmpty());
    }

    @Test
    void duplicateTargetKeysAreCountedOnceForTheGap() {
        // A duplicate TARGET key is itself a defect the key-level check fails the run on;
        // here it must not inflate the unconfirmed count, and must not appear twice.
        TargetSystemReconciliation r = ReconService.matchConfirmations(
                Stream.of("k1", "k1", "k2").toList(), Set.of("k2"), true);

        assertEquals(3, r.targetRows());
        assertEquals(1, r.unconfirmedTargetRows());
        assertEquals(List.of("k1"), r.unconfirmedAccountKeys());
    }

    @Test
    void extraConfirmationsForOtherRunsAreIgnored() {
        // Confirmations are already filtered by runId in readConfirmations, but the
        // matcher itself only joins on key — an extra key in the confirmation set that
        // matches no TARGET row is simply unused, never a defect.
        TargetSystemReconciliation r = ReconService.matchConfirmations(
                List.of("k1", "k2"), Set.of("k1", "k2", "k3", "k4"), true);

        assertTrue(r.allConfirmed());
        assertEquals(2, r.confirmedTargetRows());
    }
}