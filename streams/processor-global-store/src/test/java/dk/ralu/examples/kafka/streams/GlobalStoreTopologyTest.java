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

        inputNewMessage(TopicName.USERS, "rasmus@ralu.dk", "Rasmus Lund");
        inputNewMessage(TopicName.USERS, "vicky@ralu.dk", "Vicky Lund");
        inputNewMessage(TopicName.USERS, "jeppe@ralu.dk", "Jeppe Lund");
        inputNewMessage(TopicName.USERS, "nanna@ralu.dk", "Nanna Lund");
        inputNewMessage(TopicName.USERS, "rikke@ralu.dk", "Rikke Lund");
        inputNewMessage(TopicName.USERS, "kalle@ralu.dk", "Kalle Lund");

        inputNewMessage(TopicName.MAILS, "vicky@ralu.dk", "Hey");
        inputNewMessage(TopicName.MAILS, "rasmus@ralu.dk", "Good day");
        inputNewMessage(TopicName.MAILS, "rikke@ralu.dk", "How are you?");

        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.ENRICHED_MAILS), "vicky@ralu.dk", "Hey from Vicky Lund");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.ENRICHED_MAILS), "rasmus@ralu.dk", "Good day from Rasmus Lund");
        OutputVerifier.compareKeyValue(readOutputMessage(TopicName.ENRICHED_MAILS), "rikke@ralu.dk", "How are you? from Rikke Lund");
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
