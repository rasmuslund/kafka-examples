package dk.ralu.examples.kafka.streams;

class Name {

    static class NodeName {

        static final String SOURCE_FROM_A = "from-" + TopicName.A;
        static final String SOURCE_FROM_B = "from-" + TopicName.B;
        static final String SOURCE_FROM_C = "from-" + TopicName.C;
        static final String PROCESSOR_BRANCH = "branch";
        static final String PROCESSOR_VALUE_MODIFIER = "value-modifier";
        static final String SINK_TO_X = "to-" + TopicName.X;
    }

    static class StoreName {

    }

    static class TopicName {

        static final String A = "a";
        static final String B = "b";
        static final String C = "c";
        static final String X = "x";
    }
}
