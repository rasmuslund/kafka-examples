package dk.ralu.examples.kafka.streams;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ValueModifierProcessor extends AbstractProcessor<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValueModifierProcessor.class);

    private final String valuePrefix;

    ValueModifierProcessor(String valuePrefix) {
        this.valuePrefix = valuePrefix;
    }

    @Override
    public void process(String key, String value) {
        String newValue = valuePrefix + value + " from topic " + context().topic();
        LOGGER.info("Modified [{}] from [{}] to [{}]", key, value, newValue);
        context().forward(key, newValue);
    }
}
