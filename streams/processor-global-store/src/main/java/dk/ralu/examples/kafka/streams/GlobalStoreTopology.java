package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Name.NodeName;
import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Name.StoreName;
import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Name.TopicName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GlobalStoreTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalStoreTopology.class);

    static Topology build() {

        Topology topology = new Topology()

                .addSource(
                        NodeName.SOURCE_FROM_MAILS,
                        Serdes.String().deserializer(), Serdes.String().deserializer(),
                        TopicName.MAILS
                )

                .addGlobalStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(StoreName.USERS_GLOBAL_STORE),
                                Serdes.String(),
                                Serdes.String()
                        ).withLoggingDisabled(),
                        NodeName.SOURCE_FROM_USERS,
                        Serdes.String().deserializer(),
                        Serdes.String().deserializer(),
                        TopicName.USERS,
                        NodeName.PROCESSOR_USERS_GLOBAL_STORE_UPDATER,
                        UsersGlobalStoreUpdater::new
                )

                .addProcessor(
                        NodeName.PROCESSOR_JOIN_MAILS_AND_USERS,
                        JoinMailsAndUsersProcessor::new,
                        NodeName.SOURCE_FROM_MAILS
                )

                // Global store are automatically available to all processors - they MUST NOT be explicitly connected - e.g. by calling:
                // .connectProcessorAndStateStores(NodeName.PROCESSOR_JOIN_MAILS_AND_USERS, StoreName.USERS_GLOBAL_STORE)

                .addSink(
                        NodeName.SINK_TO_ENRICHED_MAILS,
                        TopicName.ENRICHED_MAILS,
                        Serdes.String().serializer(),
                        Serdes.String().serializer(),
                        NodeName.PROCESSOR_JOIN_MAILS_AND_USERS);

        LOGGER.info(topology.describe().toString());
        LOGGER.info("Copy topology description above into: https://zz85.github.io/kafka-streams-viz/");

        return topology;
    }

    static class Name {

        static class NodeName {

            static final String SOURCE_FROM_USERS = "from-" + TopicName.USERS;
            static final String SOURCE_FROM_MAILS = "from-" + TopicName.MAILS;
            static final String PROCESSOR_USERS_GLOBAL_STORE_UPDATER = StoreName.USERS_GLOBAL_STORE + "-updater";
            static final String PROCESSOR_JOIN_MAILS_AND_USERS = "join-mails-and-users";
            static final String SINK_TO_ENRICHED_MAILS = "to-" + TopicName.ENRICHED_MAILS;
        }

        static class StoreName {

            static final String USERS_GLOBAL_STORE = "users-global-store";
        }

        static class TopicName {

            static final String USERS = "users";
            static final String MAILS = "mails";
            static final String ENRICHED_MAILS = "enriched-mails";
        }
    }
}
