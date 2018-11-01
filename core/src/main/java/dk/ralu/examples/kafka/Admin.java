package dk.ralu.examples.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Admin {

    private static final Logger LOGGER = LoggerFactory.getLogger(Admin.class);

    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");

        try (KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) AdminClient.create(kafkaProps)) {
            // Seems like deleting and creating are async, so may have to run one of them at a time
            /*
            String topicName = "test";
            try {
                kafkaAdminClient.deleteTopics(Collections.singleton(topicName));
                LOGGER.info("Deleted topic {}", topicName);
            } catch (RuntimeException e) {
                LOGGER.warn("Unable to delete topic {}", topicName);
            }
            */
            kafkaAdminClient.createTopics(Collections.singleton(new NewTopic("test", 2, (short)1)));
            logClusterInfo(kafkaAdminClient);
            logTopics(kafkaAdminClient);
        }
    }

    private static void logTopics(KafkaAdminClient kafkaAdminClient) throws Exception {
        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics(new ListTopicsOptions().listInternal(true));
        LOGGER.info("Topics: {}", listTopicsResult.namesToListings().get());
    }

    private static void logClusterInfo(KafkaAdminClient kafkaAdminClient) throws Exception {
        DescribeClusterResult describeClusterResult = kafkaAdminClient.describeCluster(new DescribeClusterOptions());
        LOGGER.info("ClusterId: {}", describeClusterResult.clusterId().get());
        LOGGER.info("Controller: {}", toString(describeClusterResult.controller().get()));
        LOGGER.info("Nodes: {}", toString(describeClusterResult.nodes().get()));
    }

    private static String toString(Node node) {
        StringBuilder sb = new StringBuilder();
        sb.append("Node{");
        sb.append("id:").append(node.idString());
        sb.append(", host:").append(node.host());
        sb.append(", port:").append(node.port());
        sb.append(", rack:").append(node.rack());
        sb.append("}");
        return sb.toString();
    }

    private static String toString(Collection<Node> nodes) {
        return nodes.stream().map(Admin::toString).collect(Collectors.joining(", ", "{", "}"));
    }
}
