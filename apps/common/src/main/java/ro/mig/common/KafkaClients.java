package ro.mig.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * One place where this project decides how to talk to a broker.
 *
 * <p>The security block below was copy-pasted in {@code TargetSystemMock} and
 * {@code ReconService}; the Loader's move onto Kafka (docs/PLAN-CHANGES-02092026-kafka-loader.md,
 * change #2) would have made it a third copy, which is the point at which two independent
 * transports drift and one of them starts failing in a way the other cannot reproduce.
 *
 * <p>The transport is chosen by {@code KAFKA_SECURITY_PROTOCOL}: locally redpanda is
 * PLAINTEXT (the default, so the local stack is unchanged), on GCP Managed Kafka is
 * SASL_SSL/OAUTHBEARER authenticated with a Google access token via
 * {@link GcpTokenOauthCallbackHandler}. The JAAS login module must be declared even when a
 * custom callback handler is supplied, otherwise Kafka fails with "No login module found
 * for OAUTHBEARER".
 */
public final class KafkaClients {

    private KafkaClients() {
    }

    /**
     * Producer properties shared by every producer in this repo.
     *
     * <p>{@code enable.idempotence} plus broker-side retries is what replaced the Loader's
     * hand-rolled backoff loop: the client will not write a duplicate on a retried send, so
     * the retry policy stops being application code. These values deliberately match
     * {@code pipelines/common/sinks.py:KafkaTargetWriter} so the Python and Java producers
     * on this project agree about durability.
     */
    public static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        applySecurity(props);
        return props;
    }

    /**
     * Consumer properties for a bounded, one-shot read of a small topic.
     *
     * <p>A fresh group per run (the caller supplies {@code groupId}, conventionally
     * {@code <role>-<runId>}) means a re-run never skips records a previous group committed
     * past — the read is bounded by end offsets, not by a committed position, so auto-commit
     * is off and the reset is {@code earliest}.
     */
    public static Properties consumerProps(String bootstrap, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        applySecurity(props);
        return props;
    }

    /**
     * Adds the SASL_SSL/OAUTHBEARER block when {@code KAFKA_SECURITY_PROTOCOL} names
     * anything other than PLAINTEXT. Left alone otherwise, which is what keeps the local
     * redpanda stack working with no configuration at all.
     */
    public static void applySecurity(Properties props) {
        if ("PLAINTEXT".equals(securityProtocol())) {
            return;
        }
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class",
                "ro.mig.common.GcpTokenOauthCallbackHandler");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
    }

    /** The configured protocol, defaulting to PLAINTEXT so an unset environment is local. */
    public static String securityProtocol() {
        String value = System.getenv("KAFKA_SECURITY_PROTOCOL");
        return value == null || value.isBlank() ? "PLAINTEXT" : value;
    }
}
