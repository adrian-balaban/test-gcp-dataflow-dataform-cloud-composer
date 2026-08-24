package ro.mig.common;

import java.time.Instant;
import java.util.UUID;

/**
 * Run identity.
 *
 * <p>Every artefact, BigQuery row and reconciliation report is stamped with this, and
 * reconciliation always scopes to a single {@code runId}. Every run is one full snapshot
 * of the source (docs/PLAN-CHANGES-21082026.md D5) — there is no window and no run kind.
 *
 * @param runId identifies one bounded execution
 */
public record RunContext(String runId) {

    public static RunContext newRun() {
        return new RunContext(newRunId());
    }

    public static String newRunId() {
        return "run-" + Instant.now().toEpochMilli() + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }
}
