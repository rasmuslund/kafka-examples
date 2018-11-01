package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.Name.NodeName;
import dk.ralu.examples.kafka.streams.Name.TopicName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopologyBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

    static Topology build() {

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
                        BranchProcessor::new,
                        NodeName.SOURCE_FROM_C
                )

                .addProcessor(
                        NodeName.PROCESSOR_VALUE_MODIFIER,
                        () -> new ValueModifierProcessor(valuePrefix),
                        NodeName.SOURCE_FROM_A, NodeName.SOURCE_FROM_B, NodeName.PROCESSOR_BRANCH)

                .addSink(
                        NodeName.SINK_TO_X,
                        TopicName.X,
                        Serdes.String().serializer(), Serdes.String().serializer(),
                        NodeName.PROCESSOR_VALUE_MODIFIER, NodeName.PROCESSOR_BRANCH);

        LOGGER.info(topology.describe().toString());
        LOGGER.info("Copy topology description above into: https://zz85.github.io/kafka-streams-viz/");

        return topology;
    }
}
