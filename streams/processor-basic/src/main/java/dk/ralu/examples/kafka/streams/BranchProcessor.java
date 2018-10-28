package dk.ralu.examples.kafka.streams;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BranchProcessor extends AbstractProcessor<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BranchProcessor.class);

    @Override
    public void process(String key, String value) {

        String targetChildName = (key.contains("3")) ? NodeName.SINK_TO_X : NodeName.PROCESSOR_VALUE_MODIFIER;
        LOGGER.info("Forwarding entry with key [{}] to child node named [{}]", key, targetChildName);
        context().forward(key, value, To.child(targetChildName));
    }
}
