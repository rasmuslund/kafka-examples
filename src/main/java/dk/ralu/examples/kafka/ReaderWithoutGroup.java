package dk.ralu.examples.kafka;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderWithoutGroup {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderWithoutGroup.class);

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");

        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // We ***don't*** assign ourselves to a group

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps)) {

            KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = new KafkaConsumerWrapper<>(kafkaConsumer);
            LOGGER.info("Created consumer with config {}", kafkaProps);

            String topic = "test";
            List<TopicPartition> topicPartitions = kafkaConsumerWrapper.getTopicPartitions(topic);
            kafkaConsumer.assign(topicPartitions); // ***We*** choose which partitions we want to poll from
            LOGGER.info("Subscribed to topics partitions {}", topicPartitions);

            while (true) {
                // controls how long poll() will block if data is not available in the consumer buffer
                int timeout = 100;
                ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Read record with key [{}], value [{}], and headers {} from topic {}, partition {}, and offset {}",
                                record.key(), record.value(), record.headers(), record.topic(), record.partition(), record.offset());
                }
            }
        }
    }

}