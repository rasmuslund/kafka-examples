package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.TopicName;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KeyValueStoreAndPunctuateTopologyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueStoreAndPunctuateTopologyTest.class);
    private TopologyTestDriver driver;
    private ConsumerRecordFactory<String, String> inputFactory;

    @BeforeEach
    void beforeEach() {

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, KeyValueStoreAndPunctuateTopologyTest.class.getSimpleName());
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored.com:9091");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        driver = new TopologyTestDriver(KeyValueStoreAndPunctuateTopology.build(), config);

        inputFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    }

    @Test
    void test() {

        inputNewMessage(TopicName.INCOMING_MAILS, "vicky@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "jeppe@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "rikke@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "vicky@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "jeppe@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "vicky@example.com", "Message");

        // Makes 1st punctuate fire (fires every 5 minutes wall clock time)
        driver.advanceWallClockTime(TimeUnit.MINUTES.toMillis(6));

        inputNewMessage(TopicName.INCOMING_MAILS, "jeppe@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "rikke@example.com", "Message");
        inputNewMessage(TopicName.INCOMING_MAILS, "rikke@example.com", "Message");

        // Makes 2nd punctuate fire
        driver.advanceWallClockTime(TimeUnit.MINUTES.toMillis(5));

        // 1st punctuate fired
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.MAIL_COUNTS), "jeppe@example.com", "Count was 2");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.MAIL_COUNTS), "rikke@example.com", "Count was 1");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.MAIL_COUNTS), "vicky@example.com", "Count was 3");

        // 2nd punctuate fired
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.MAIL_COUNTS), "jeppe@example.com", "Count was 1");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.MAIL_COUNTS), "rikke@example.com", "Count was 2");

        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "vicky@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "jeppe@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "rikke@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "vicky@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "jeppe@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "vicky@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "jeppe@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "rikke@example.com", "Message");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.OUTGOING_MAILS), "rikke@example.com", "Message");
    }

    private void inputNewMessage(String topicName, String key, String value) {
        ConsumerRecord<byte[], byte[]> record = inputFactory.create(topicName, key, value);
        LOGGER.info("Inputting new record [{}] => [{}] on topic [{}]", key, value, topicName);
        driver.pipeInput(record);
    }

    private ProducerRecord<String, String> readOutputMessage(String topicName) {
        ProducerRecord<String, String> record = driver.readOutput(topicName, new StringDeserializer(), new StringDeserializer());
        LOGGER.info("Read output record [{}] => [{}] from topic [{}]", record.key(), record.value(), topicName);
        return record;
    }
}
