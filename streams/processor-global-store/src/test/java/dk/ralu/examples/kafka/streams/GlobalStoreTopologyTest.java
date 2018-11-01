package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Name.TopicName;
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

class GlobalStoreTopologyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalStoreTopologyTest.class);
    private TopologyTestDriver driver;
    private ConsumerRecordFactory<String, String> inputFactory;

    @BeforeEach
    void beforeEach() {

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, GlobalStoreTopologyTest.class.getSimpleName());
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored.com:9091");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        driver = new TopologyTestDriver(GlobalStoreTopology.build(), config);

        inputFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    }

    @Test
    void test() {

        inputNewMessage(TopicName.USERS, "rasmus@example.com", "Rasmus Lund");
        inputNewMessage(TopicName.USERS, "vicky@example.com", "Vicky Lund");
        inputNewMessage(TopicName.USERS, "jeppe@example.com", "Jeppe Lund");
        inputNewMessage(TopicName.USERS, "nanna@example.com", "Nanna Lund");
        inputNewMessage(TopicName.USERS, "rikke@example.com", "Rikke Lund");
        inputNewMessage(TopicName.USERS, "kalle@example.com", "Kalle Lund");

        inputNewMessage(TopicName.MAILS, "vicky@example.com", "Hey");
        inputNewMessage(TopicName.MAILS, "rasmus@example.com", "Good day");
        inputNewMessage(TopicName.MAILS, "rikke@example.com", "How are you?");

        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.ENRICHED_MAILS), "vicky@example.com", "Hey from Vicky Lund");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.ENRICHED_MAILS), "rasmus@example.com", "Good day from Rasmus Lund");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.ENRICHED_MAILS), "rikke@example.com", "How are you? from Rikke Lund");
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
