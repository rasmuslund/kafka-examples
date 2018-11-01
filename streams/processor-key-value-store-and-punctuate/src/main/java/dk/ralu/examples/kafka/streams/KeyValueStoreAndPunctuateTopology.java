package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.CustomSerde;
import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.NodeName;
import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.StoreName;
import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.TopicName;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
class KeyValueStoreAndPunctuateTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueStoreAndPunctuateTopology.class);

    static Topology build() {

        Topology topology = new Topology()

                .addSource(
                        NodeName.SOURCE_FROM_INCOMING_MAILS,
                        CustomSerde.MAIL_ADDRESS.deserializer(),
                        CustomSerde.MESSAGE.deserializer(),
                        TopicName.INCOMING_MAILS
                )

                .addProcessor(
                        NodeName.PROCESSOR_MAIL_COUNTER,
                        MailCounter::new,
                        NodeName.SOURCE_FROM_INCOMING_MAILS
                )

                .addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(StoreName.MAIL_ADDRESS_TO_COUNT_STORE),
                                CustomSerde.MAIL_ADDRESS,
                                CustomSerde.MAIL_COUNT
                        ),
                        NodeName.PROCESSOR_MAIL_COUNTER
                )

                .addSink(
                        NodeName.SINK_MAILS,
                        TopicName.OUTGOING_MAILS,
                        CustomSerde.MAIL_ADDRESS.serializer(),
                        CustomSerde.MESSAGE.serializer(),
                        NodeName.PROCESSOR_MAIL_COUNTER)

                .addSink(
                        NodeName.SINK_COUNTS,
                        TopicName.MAIL_COUNTS,
                        CustomSerde.MAIL_ADDRESS.serializer(),
                        CustomSerde.MAIL_COUNT_RESULT.serializer(),
                        NodeName.PROCESSOR_MAIL_COUNTER);

        LOGGER.info(topology.describe().toString());
        LOGGER.info("Copy topology description above into: https://zz85.github.io/kafka-streams-viz/");

        return topology;
    }

    static class Constant {

        static class NodeName {

            static final String SOURCE_FROM_INCOMING_MAILS = "from-" + TopicName.INCOMING_MAILS;
            static final String PROCESSOR_MAIL_COUNTER = "mail-counter";
            static final String SINK_MAILS = "to-" + TopicName.OUTGOING_MAILS;
            static final String SINK_COUNTS = "to-" + TopicName.MAIL_COUNTS;
        }

        static class StoreName {

            static final String MAIL_ADDRESS_TO_COUNT_STORE = "mail-address-to-count-store";
        }

        static class TopicName {

            static final String INCOMING_MAILS = "mails-in";
            static final String OUTGOING_MAILS = "mails-out";
            static final String MAIL_COUNTS = "mail-counts";
        }

        static class CustomSerde {

            static final Serde MAIL_ADDRESS = Serdes.String();
            static final Serde MESSAGE = Serdes.String();
            static final Serde MAIL_COUNT = Serdes.Integer();
            static final Serde MAIL_COUNT_RESULT = Serdes.String();
        }
    }
}
