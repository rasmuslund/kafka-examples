package dk.ralu.examples.kafka.streams;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalStoreMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalStoreMain.class);

    public static void main(String[] args) {

        /*

        To insert some messages into input topic users:
            ./kafka-console-producer.sh --broker-list localhost:9092 --topic users --property "parse.key=true" --property "key.separator=:"

        To insert some messages into input topic users:
            ./kafka-console-producer.sh --broker-list localhost:9092 --topic mails --property "parse.key=true" --property "key.separator=:"

        To see messages output to topic enriched-mails:
            ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic enriched-mails --from-beginning --property print.key=true --property key.separator=":"

        // To reset the streams app (don't add users as an input topic as it is always read from the beginning because it is input for the global state store):
            ./kafka-streams-application-reset.sh --bootstrap-servers localhost:9092 --application-id GlobalStoreMain --input-topics mails

        */

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, GlobalStoreTopology.class.getSimpleName());
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        KafkaStreams kafkaStreams = new KafkaStreams(GlobalStoreTopology.build(), config);
        // Always cleanup the content of the global state store at startup
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        while (true) {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException ignored) {
            }
            LOGGER.info("Metrics: {}", toString(kafkaStreams.metrics()));
        }
    }

    private static String toString(Map<MetricName, ? extends Metric> metrics) {
        DecimalFormat df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);
        StringBuilder sb = new StringBuilder();
        sb.append("*** Metrics at ").append(Instant.now()).append(" ***");
        Set<String> lines = new TreeSet<>();
        for (Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            Metric metric = entry.getValue();
            Object metricValue = metric.metricValue();
            String metricValueAsString = null;
            if (!(metricValue instanceof  Double)) {
                metricValueAsString = "" + metricValue;
            }
            if (metricValue instanceof Double) {
                Double doubleValue = (Double) metricValue;
                if (doubleValue == 0.0 || doubleValue.isInfinite()) {
                    continue;
                }
                metricValueAsString = df.format(doubleValue);
            }
            Map<String, String> tags = entry.getKey().tags();
            //tags.remove("client-id");
            lines.add(entry.getKey().group() + "." + entry.getKey().name() + tags + "=" + metricValueAsString);
        }
        for (String line : lines) {
            sb.append(line).append('\n');
        }
        return sb.toString();
    }
}
