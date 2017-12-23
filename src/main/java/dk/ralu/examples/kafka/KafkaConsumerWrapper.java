package dk.ralu.examples.kafka;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWrapper<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWrapper.class);

    private final KafkaConsumer<K, V> kafkaConsumer;

    public KafkaConsumerWrapper(KafkaConsumer<K, V> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public void logCurrentBeginningAndEndOffsetsForTopicPartitions(String topic) {
        List<TopicPartition> topicPartitions = getTopicPartitions(topic);
        LOGGER.info("Beginning offsets {}", kafkaConsumer.beginningOffsets(topicPartitions));
        LOGGER.info("End offsets {}", kafkaConsumer.endOffsets(topicPartitions));
    }

    public List<TopicPartition> getTopicPartitions(String topic) {
        return kafkaConsumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
    }
}
