package dk.ralu.examples.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");
        //kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
        //kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");

        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {

            LOGGER.info("Created producer with config {}", kafkaProps);

            String topic = "test";
            Integer partition = 0;
            String key = "the key";
            String value = "message content";
            //List<Header> headers = Arrays.asList(new RecordHeader("h1", "h1-value".getBytes("UTF-8")));
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, value/*, headers*/);
            LOGGER.info("Created record with topic [{}], partition [{}], key [{}], and value [{}]", topic, partition, key, value);

            Future<RecordMetadata> sendResponseFuture = kafkaProducer.send(record);
            LOGGER.info("Send message");

            LOGGER.info("Message successfully sent (offset {})", sendResponseFuture.get().offset());

            List<PartitionInfo> partitionInfoList = kafkaProducer.partitionsFor("test");
            LOGGER.info("Partitions on topic test is {}", partitionInfoList);
        }
    }
}