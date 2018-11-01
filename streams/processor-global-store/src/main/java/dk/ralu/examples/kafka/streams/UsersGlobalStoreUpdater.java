package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.StoreName;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

class UsersGlobalStoreUpdater implements Processor<String, String> {

    private KeyValueStore<String, String> usersGlobalStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        usersGlobalStore = (KeyValueStore<String, String>) context.getStateStore(StoreName.USERS_GLOBAL_STORE);
    }

    @Override
    public void process(String key, String value) {
        usersGlobalStore.put(key, value);
    }

    @Override
    public void close() {
        usersGlobalStore = null;
    }
}
