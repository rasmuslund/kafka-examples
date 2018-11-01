package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.BasicProcessorTopology.Constant.TopicName;
import java.util.Properties;
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

class BasicProcessorTopologyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicProcessorTopologyTest.class);
    private TopologyTestDriver driver;
    private ConsumerRecordFactory<String, String> inputFactory;

    @BeforeEach
    void beforeEach() {

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, BasicProcessorTopologyTest.class.getSimpleName());
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored.com:9091");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        driver = new TopologyTestDriver(BasicProcessorTopology.build(), config);

        inputFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    }

    @Test
    void test() {

        inputNewMessage(TopicName.A, "key-1", "value-1");
        inputNewMessage(TopicName.B, "key-2", "value-2");
        inputNewMessage(TopicName.C, "key-3", "value-3");
        inputNewMessage(TopicName.C, "key-4", "value-4");

        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.X), "key-1", "*** value-1 from topic a");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.X), "key-2", "*** value-2 from topic b");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.X), "key-3", "value-3"); // see BranchProcessor
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.X), "key-4", "*** value-4 from topic c");
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
