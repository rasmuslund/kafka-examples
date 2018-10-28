package dk.ralu.examples.kafka.streams;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

public class ProcessorApiEx01 {

    public static void main(String[] args) {

        String valuePrefix = "*** ";

        Topology topology = new Topology()

                .addSource(
                        NodeName.SOURCE_FROM_A,
                        Serdes.String().deserializer(), Serdes.String().deserializer(),
                        TopicName.A
                        )

                .addSource(
                        NodeName.SOURCE_FROM_B,
                        Serdes.String().deserializer(), Serdes.String().deserializer(),
                        TopicName.B)

                .addSource(
                        NodeName.SOURCE_FROM_C,
                        Serdes.String().deserializer(), Serdes.String().deserializer(),
                        TopicName.C)

                .addProcessor(
                        NodeName.PROCESSOR_BRANCH,
                        Branch::new,
                        NodeName.SOURCE_FROM_C
                )

                .addProcessor(
                        NodeName.PROCESSOR_VALUE_MODIFIER,
                        () -> new ValueModifier(valuePrefix),
                        NodeName.SOURCE_FROM_A, NodeName.SOURCE_FROM_B, NodeName.PROCESSOR_BRANCH)

                .addSink(
                        NodeName.SINK_TO_X,
                        TopicName.X,
                        Serdes.String().serializer(), Serdes.String().serializer(),
                        NodeName.PROCESSOR_VALUE_MODIFIER, NodeName.PROCESSOR_BRANCH);

        System.out.println(topology.describe());

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, ProcessorApiEx01.class.getSimpleName());
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ignored.com:9091");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        TopologyTestDriver driver = new TopologyTestDriver(topology, config);
        ConsumerRecordFactory<String, String> inputFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

        driver.pipeInput(inputFactory.create(TopicName.A, "key-1", "value-1"));
        driver.pipeInput(inputFactory.create(TopicName.B, "key-2", "value-2"));
        driver.pipeInput(inputFactory.create(TopicName.C, "key-3", "value-3"));
        driver.pipeInput(inputFactory.create(TopicName.C, "key-4", "value-4"));

        for (int i = 0; i < 4; i++) {
            ProducerRecord<String, String> outputRecord = driver.readOutput(TopicName.X, new StringDeserializer(), new StringDeserializer());
            System.out.println(outputRecord.key() + " => " + outputRecord.value());
        }

    }

    static class TopicName {

        static final String A = "a";
        static final String B = "b";
        static final String C = "c";
        static final String X = "x";
    }

    static class NodeName {

        static final String SOURCE_FROM_A = "from-" + TopicName.A;
        static final String SOURCE_FROM_B = "from-" + TopicName.B;
        static final String SOURCE_FROM_C = "from-" + TopicName.C;
        static final String PROCESSOR_BRANCH = "branch";
        static final String PROCESSOR_VALUE_MODIFIER = "value-modifier";
        static final String SINK_TO_X = "to-" + TopicName.X;
    }

    static class Branch extends AbstractProcessor<String, String> {

        @Override
        public void process(String key, String value) {
            if (key.hashCode() % 2 == 0) {
                context().forward(key, value, To.child(NodeName.PROCESSOR_VALUE_MODIFIER));
            } else {
                context().forward(key, value, To.child(NodeName.SINK_TO_X));
            }
        }
    }

    static class ValueModifier extends AbstractProcessor<String, String> {

        private final String valuePrefix;

        ValueModifier(String valuePrefix) {
            this.valuePrefix = valuePrefix;
        }

        @Override
        public void process(String key, String value) {
            System.out.println("Prefixed value of key " + key);
            context().forward(key, valuePrefix + value + " from topic " + context().topic());
        }
    }

}
