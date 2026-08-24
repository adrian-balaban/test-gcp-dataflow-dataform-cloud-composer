package ro.mig.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Minimal BigQuery query client over the REST {@code jobs.query} API, using only the JDK.
 *
 * <p>Deliberately not the google-cloud-bigquery Java client: reconciliation only ever runs
 * {@code SELECT}s, and the full client would pull a large dependency tree into an app whose
 * entire job is to count things. Same adapter pattern as {@link HttpObjectStore} — point it at
 * the emulator locally, or at {@code https://bigquery.googleapis.com} with a bearer token on
 * real GCP.
 */
public final class BigQueryRest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String host;
    private final String project;
    private final HttpClient http;
    private final Supplier<String> bearerToken;

    public BigQueryRest(String host, String project, Supplier<String> bearerToken) {
        this.host = host.replaceAll("/+$", "");
        this.project = project;
        this.bearerToken = bearerToken;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    public static BigQueryRest local(String host, String project) {
        return new BigQueryRest(host, project, () -> null);
    }

    /**
     * Same seam as {@link HttpObjectStore#fromEnv}: {@code MIG_GCS_TOKEN} when the caller
     * exports one, otherwise the pod's own metadata-server token. See {@link GcpToken}.
     */
    public static BigQueryRest fromEnv(String host, String project) {
        return new BigQueryRest(host, project, GcpToken.supplier());
    }

    /** Run a standard-SQL query and return the rows as ordered column→value maps. */
    public List<Map<String, String>> query(String sql) {
        return query(sql, Map.of());
    }

    /**
     * Run a parameterised standard-SQL query — the only safe way to pass a run id.
     *
     * <p>Named parameters are referenced as {@code @name} in the SQL and sent as
     * {@code queryParameters}, so a value can never be reinterpreted as SQL. The Python
     * side already did this (the DAG's {@code assert_run_balanced} uses
     * {@code ScalarQueryParameter}); this brings the Java port back in line.
     *
     * <p>All parameters are typed STRING: every value this codebase parameterises
     * (run ids, lane names) is a string, and BigQuery will coerce in comparisons.
     */
    public List<Map<String, String>> query(String sql, Map<String, String> params) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("query", sql);
        body.put("useLegacySql", false);
        body.put("timeoutMs", 120_000);

        if (!params.isEmpty()) {
            var array = body.putArray("queryParameters");
            params.forEach((name, value) -> {
                ObjectNode p = array.addObject();
                p.put("name", name);
                p.putObject("parameterType").put("type", "STRING");
                // A null value must be sent as a JSON null, not the string "null".
                ObjectNode holder = p.putObject("parameterValue");
                if (value == null) {
                    holder.putNull("value");
                } else {
                    holder.put("value", value);
                }
            });
        }

        HttpRequest.Builder builder = HttpRequest
                .newBuilder(URI.create(host + "/bigquery/v2/projects/" + project + "/queries"))
                .timeout(Duration.ofSeconds(180))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()));

        String token = bearerToken.get();
        if (token != null && !token.isBlank()) {
            builder.header("Authorization", "Bearer " + token);
        }

        HttpResponse<String> response;
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BigQueryException("query failed: " + sql, e);
        }

        if (response.statusCode() / 100 != 2) {
            throw new BigQueryException(
                    "query returned HTTP " + response.statusCode() + ": " + response.body()
                            + "\nSQL: " + sql, null);
        }

        try {
            JsonNode root = MAPPER.readTree(response.body());

            // A server-side timeout returns HTTP 200 with jobComplete:false and no rows.
            // Returning those as "the answer" would let a timed-out count of 0 silently
            // satisfy the balancing equation — a wrong run reported as a good one. Fail
            // loudly instead: recon must never conclude from a partial result.
            JsonNode complete = root.path("jobComplete");
            if (complete.isBoolean() && !complete.asBoolean()) {
                throw new BigQueryException(
                        "query did not complete within the timeout (jobComplete=false); "
                                + "refusing to return a partial result.\nSQL: " + sql, null);
            }

            List<String> columns = new ArrayList<>();
            for (JsonNode field : root.path("schema").path("fields")) {
                columns.add(field.path("name").asText());
            }
            List<Map<String, String>> rows = new ArrayList<>();
            collectRows(root, columns, rows);

            // BigQuery returns at most one page per response and hands back a pageToken
            // for the rest. Ignoring it silently truncates: today every recon query is an
            // aggregate returning a single row, so nothing breaks — but the first
            // non-aggregate query (a per-key orphan listing, say) would quietly reconcile
            // against a fraction of the data and report agreement. A reconciliation
            // service that can under-read without saying so is worse than one that fails.
            String pageToken = root.path("pageToken").asText("");
            String jobId = root.path("jobReference").path("jobId").asText("");
            while (!pageToken.isBlank() && !jobId.isBlank()) {
                JsonNode page = fetchPage(jobId, pageToken);
                collectRows(page, columns, rows);
                pageToken = page.path("pageToken").asText("");
            }
            return rows;
        } catch (IOException e) {
            throw new BigQueryException("could not parse query response", e);
        }
    }

    private void collectRows(JsonNode root, List<String> columns, List<Map<String, String>> out) {
        for (JsonNode row : root.path("rows")) {
            Map<String, String> record = new LinkedHashMap<>();
            JsonNode values = row.path("f");
            for (int i = 0; i < columns.size() && i < values.size(); i++) {
                JsonNode value = values.get(i).path("v");
                record.put(columns.get(i), value.isNull() ? null : value.asText());
            }
            out.add(record);
        }
    }

    /** getQueryResults for one further page of an already-completed job. */
    private JsonNode fetchPage(String jobId, String pageToken) {
        // project and jobId are BigQuery identifiers (validated at Args.parse / assigned by
        // the API); pageToken is opaque base64 and is percent-encoded.
        String url = host + "/bigquery/v2/projects/" + project + "/queries/" + jobId
                + "?pageToken=" + URLEncoder.encode(pageToken, StandardCharsets.UTF_8);
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(180))
                .GET();
        String token = bearerToken.get();
        if (token != null && !token.isBlank()) {
            builder.header("Authorization", "Bearer " + token);
        }
        try {
            HttpResponse<String> response =
                    http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new BigQueryException(
                        "getQueryResults returned HTTP " + response.statusCode() + ": "
                                + response.body(), null);
            }
            return MAPPER.readTree(response.body());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BigQueryException("could not read result page " + pageToken, e);
        }
    }

    /** Convenience for the very common single-number reconciliation query. */
    public long count(String sql) {
        return count(sql, Map.of());
    }

    /** Parameterised {@link #count(String)} — see {@link #query(String, Map)}. */
    public long count(String sql, Map<String, String> params) {
        List<Map<String, String>> rows = query(sql, params);
        if (rows.isEmpty()) {
            return 0;
        }
        String value = rows.get(0).values().iterator().next();
        return value == null ? 0 : Long.parseLong(value);
    }

    public static final class BigQueryException extends RuntimeException {
        public BigQueryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
