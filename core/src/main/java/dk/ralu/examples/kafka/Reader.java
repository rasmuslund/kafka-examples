package dk.ralu.examples.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reader.class);

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");
        //kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
        //kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");

        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // org.apache.kafka.common.serialization.StringDeserializer.class.getCanonicalName()

        // Almost all consumers should be part of a consumer group
        kafkaProps.put("group.id", "my-test-group");
        kafkaProps.put("auto.offset.reset",
                       "earliest"); // if no offset for given partition of the given consumer group, then go back as far as possible - alternative is "latest", which just starts at the latest message

        kafkaProps.put("auto.commit.offset", false);

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps)) {

            KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = new KafkaConsumerWrapper<>(kafkaConsumer);

            LOGGER.info("Created consumer with config {}", kafkaProps);

            String topic = "test";
            List<String> topicList = Collections.singletonList(topic);
            kafkaConsumer.subscribe(topicList, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    LOGGER.info("Rebalance: Lost partitions {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    LOGGER.info("Rebalance: Got partitions {}", partitions);
                }
            });
            LOGGER.info("Subscribed to topics {}", topicList);

            kafkaConsumerWrapper.logCurrentBeginningAndEndOffsetsForTopicPartitions(topic);

            while (true) {
                // controls how long poll() will block if data is not available in the consumer buffer
                int timeout = 100;
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(timeout));
/*
                if (isFirstLoop) {
                    // Seek must be called after first poll, as first poll is what assigns us some partitions in the consumer group
                    kafkaConsumer.seekToBeginning(kafkaConsumerWrapper.getTopicPartitions(topic));
                    isFirstLoop = false;
                    continue;
                }
*/
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Read record with key [{}], value [{}], and headers {} from topic {}, partition {}, and offset {}",
                                record.key(), record.value(), record.headers(), record.topic(), record.partition(), record.offset());
                }
                kafkaConsumer.commitAsync(); // hopefully succeeds - or else we might end up having a lot to reprocess after a crash
                // Note: a callback can be sent in as parameter to commitAsync()
                // kafkaConsumer.commitSync(); // Commits the records we have polled above, may throw CommitFailedException
            }
        }
    }

}