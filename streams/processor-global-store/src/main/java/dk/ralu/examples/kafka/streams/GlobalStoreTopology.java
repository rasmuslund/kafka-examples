package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.CustomSerde;
import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.NodeName;
import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.StoreName;
import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.TopicName;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
class GlobalStoreTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalStoreTopology.class);

    static Topology build() {

        Topology topology = new Topology()

                .addGlobalStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(StoreName.USERS_GLOBAL_STORE),
                                CustomSerde.MAIL_ADDRESS,
                                CustomSerde.USER_FULL_NAME
                        ).withLoggingDisabled(),
                        NodeName.SOURCE_FROM_USERS,
                        CustomSerde.MAIL_ADDRESS.deserializer(),
                        CustomSerde.USER_FULL_NAME.deserializer(),
                        TopicName.USERS,
                        NodeName.PROCESSOR_USERS_GLOBAL_STORE_UPDATER,
                        UsersGlobalStoreUpdater::new
                )

                .addSource(
                        NodeName.SOURCE_FROM_MAILS,
                        CustomSerde.MAIL_ADDRESS.deserializer(),
                        CustomSerde.ORIGINAL_MESSAGE.deserializer(),
                        TopicName.MAILS
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
                        CustomSerde.MAIL_ADDRESS.serializer(),
                        CustomSerde.ENRICHED_MESSAGE.serializer(),
                        NodeName.PROCESSOR_JOIN_MAILS_AND_USERS);

        LOGGER.info(topology.describe().toString());
        LOGGER.info("Copy topology description above into: https://zz85.github.io/kafka-streams-viz/");

        return topology;
    }

    static class Constant {

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

        static class CustomSerde {

            static final Serde MAIL_ADDRESS = Serdes.String();
            static final Serde USER_FULL_NAME = Serdes.String();
            static final Serde ORIGINAL_MESSAGE = Serdes.String();
            static final Serde ENRICHED_MESSAGE = Serdes.String();
        }
    }
}
