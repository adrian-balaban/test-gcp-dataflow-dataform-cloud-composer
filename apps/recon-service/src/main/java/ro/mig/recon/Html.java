package ro.mig.recon;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Human-readable rendering of the reconciliation report.
 *
 * <p>Machine-readable output <em>plus</em> a human summary: the JSON is what CI asserts
 * against, this is what gets looked at when a run fails at 2am. Deliberately a single
 * self-contained file with no external assets, so it can be opened straight from the object
 * store.
 */
final class Html {

    private Html() {
    }

    static String render(JsonNode report) {
        JsonNode eq = report.path("balancingEquation");
        boolean balanced = eq.path("balances").asBoolean();

        StringBuilder sb = new StringBuilder();
        sb.append("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
        sb.append("<title>Reconciliation — ").append(esc(report.path("runId").asText())).append("</title>");
        sb.append("<style>");
        sb.append("body{font:14px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:56rem;padding:0 1rem;color:#1b1b1b}");
        sb.append("h1{font-size:1.5rem;margin-bottom:.25rem}h2{font-size:1.05rem;margin-top:2rem;border-bottom:1px solid #ddd;padding-bottom:.3rem}");
        sb.append(".sub{color:#666;margin-top:0}");
        sb.append(".verdict{padding:.85rem 1rem;border-radius:6px;font-weight:600;margin:1.25rem 0}");
        sb.append(".ok{background:#dff0d8;border:1px solid #3c763d;color:#24632a}");
        sb.append(".fail{background:#f2dede;border:1px solid #a94442;color:#a94442}");
        sb.append("table{border-collapse:collapse;width:100%;margin:.5rem 0}");
        sb.append("th,td{text-align:left;padding:.4rem .6rem;border-bottom:1px solid #eee}");
        sb.append("th{background:#fafafa;font-weight:600}td.n{text-align:right;font-variant-numeric:tabular-nums}");
        sb.append("code{background:#f4f4f4;padding:.1rem .35rem;border-radius:3px}");
        sb.append("</style></head><body>");

        sb.append("<h1>Reconciliation report</h1>");
        sb.append("<p class=\"sub\">MIG 000001-1 &middot; run <code>")
                .append(esc(report.path("runId").asText())).append("</code></p>");

        sb.append("<div class=\"verdict ").append(balanced ? "ok" : "fail").append("\">");
        sb.append(balanced
                ? "PASS — the balancing equation closes."
                : "FAIL — the balancing equation does not close (off by " + eq.path("imbalance").asLong() + ").");
        sb.append("</div>");

        // Two doors — only the presentation groups them.
        long rejected = eq.path("rejected").asLong();
        long notMigrated = rejected;

        sb.append("<h2>Balancing equation</h2>");
        sb.append("<p><code>SRC_read = migrated + not_migrated</code></p>");
        sb.append("<table><tr><th>Door</th><th>Records</th></tr>");
        row(sb, "SRC read (from the Extractor's .RPT)", eq.path("srcRead").asLong());
        row(sb, "migrated", eq.path("written").asLong());
        row(sb, "not migrated", notMigrated);
        // Plain text, not HTML entities: row() escapes its label.
        row(sb, "— of which rejected", rejected);
        row(sb, "accounted", eq.path("accounted").asLong());
        sb.append("</table>");

        section(sb, "Source reconciliation", report.path("sourceReconciliation"));
        section(sb, "Transformation / load reconciliation", report.path("transformationLoadReconciliation"));

        JsonNode ts = report.path("targetSystemReconciliation");
        if (ts.path("enabled").asBoolean(false)) {
            boolean allConfirmed = ts.path("allTargetRowsConfirmed").asBoolean(false);
            sb.append("<h2>Target System reconciliation</h2>");
            sb.append("<p>Every TARGET row checked against the confirmation stream Target System "
                    + "published back &mdash; closing the gap between <em>posted</em> and "
                    + "<em>persisted</em>.</p>");
            sb.append("<div class=\"verdict ").append(allConfirmed ? "ok" : "fail").append("\">");
            sb.append(allConfirmed
                    ? "PASS — Target System confirmed every TARGET row."
                    : "FAIL — " + ts.path("unconfirmedTargetRows").asLong()
                            + " TARGET row(s) have no matching confirmation.");
            sb.append("</div>");
            sb.append("<table><tr><th>Measure</th><th>Value</th></tr>");
            row(sb, "target rows", ts.path("targetRows").asLong());
            row(sb, "confirmations seen", ts.path("confirmations").asLong());
            row(sb, "confirmed TARGET rows", ts.path("confirmedTargetRows").asLong());
            row(sb, "unconfirmed TARGET rows", ts.path("unconfirmedTargetRows").asLong());
            sb.append("</table>");
            JsonNode unconfirmed = ts.path("unconfirmedAccountKeys");
            if (!unconfirmed.isEmpty()) {
                sb.append("<h3>Unconfirmed account keys</h3><table><tr><th>account key</th></tr>");
                for (JsonNode k : unconfirmed) {
                    sb.append("<tr><td><code>").append(esc(k.asText())).append("</code></td></tr>");
                }
                sb.append("</table>");
            }
        }

        sb.append("<h2>Rejects by reason</h2>");
        if (report.path("rejectsByReason").isEmpty()) {
            sb.append("<p>None.</p>");
        } else {
            sb.append("<table><tr><th>Stage</th><th>Reason</th><th>Count</th></tr>");
            for (JsonNode r : report.path("rejectsByReason")) {
                sb.append("<tr><td>").append(esc(r.path("stage").asText()))
                        .append("</td><td><code>").append(esc(r.path("reason").asText()))
                        .append("</code></td><td class=\"n\">").append(r.path("count").asLong())
                        .append("</td></tr>");
            }
            sb.append("</table>");
        }

        JsonNode mig = report.path("migrability");
        sb.append("<h2>Migrability</h2><table><tr><th>Measure</th><th>Value</th></tr>");
        row(sb, "candidates", mig.path("candidates").asLong());
        row(sb, "migrated", mig.path("migrated").asLong());
        row(sb, "blocked by data quality", mig.path("blockedByDataQuality").asLong());
        sb.append("<tr><td>migrability rate (of in-scope records)</td><td class=\"n\">")
                .append(mig.path("migrabilityRate").asDouble()).append("%</td></tr>");
        sb.append("</table>");

        if (!mig.path("balanceBands").isEmpty()) {
            sb.append("<h2>Balance bands</h2><table><tr><th>Band</th><th>Accounts</th></tr>");
            for (JsonNode b : mig.path("balanceBands")) {
                sb.append("<tr><td>").append(esc(b.path("band").asText()))
                        .append("</td><td class=\"n\">").append(b.path("count").asLong())
                        .append("</td></tr>");
            }
            sb.append("</table>");
        }

        sb.append("<p class=\"sub\" style=\"margin-top:2rem\">Generated ")
                .append(esc(report.path("generatedAt").asText())).append("</p>");
        sb.append("</body></html>");
        return sb.toString();
    }

    private static void section(StringBuilder sb, String title, JsonNode node) {
        sb.append("<h2>").append(esc(title)).append("</h2><table><tr><th>Measure</th><th>Value</th></tr>");
        node.fields().forEachRemaining(e ->
                sb.append("<tr><td>").append(esc(camelToWords(e.getKey())))
                        .append("</td><td class=\"n\">").append(e.getValue().asLong())
                        .append("</td></tr>"));
        sb.append("</table>");
    }

    private static void row(StringBuilder sb, String label, long value) {
        sb.append("<tr><td>").append(esc(label)).append("</td><td class=\"n\">").append(value).append("</td></tr>");
    }

    private static String camelToWords(String name) {
        return name.replaceAll("([a-z])([A-Z])", "$1 $2").toLowerCase();
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
