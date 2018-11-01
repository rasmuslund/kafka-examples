package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.NodeName;
import dk.ralu.examples.kafka.streams.KeyValueStoreAndPunctuateTopology.Constant.StoreName;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

class MailCounter implements Processor<String, String> {

    private KeyValueStore<String, Integer> mailAddressToCountStore;
    private ProcessorContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        mailAddressToCountStore = (KeyValueStore<String, Integer>) context.getStateStore(StoreName.MAIL_ADDRESS_TO_COUNT_STORE);
        context.schedule(TimeUnit.MINUTES.toMillis(5), PunctuationType.WALL_CLOCK_TIME, this::onPunctuate);
    }

    @Override
    public void process(String mailAddress, String message) {
        Integer count = mailAddressToCountStore.get(mailAddress);
        count = (count == null ? 1 : ++count);
        mailAddressToCountStore.put(mailAddress, count);
        context.forward(mailAddress, message, To.child(NodeName.SINK_MAILS));
    }

    @Override
    public void close() {
        mailAddressToCountStore = null;
    }

    private void onPunctuate(long timestamp) {
        try (KeyValueIterator<String, Integer> iterator = mailAddressToCountStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Integer> mailAddressToCountEntry = iterator.next();
                String valueToForward = "Count was " + mailAddressToCountEntry.value;
                context.forward(mailAddressToCountEntry.key, valueToForward, To.child(NodeName.SINK_COUNTS));
                mailAddressToCountStore.delete(mailAddressToCountEntry.key);
            }
        }
    }
}
