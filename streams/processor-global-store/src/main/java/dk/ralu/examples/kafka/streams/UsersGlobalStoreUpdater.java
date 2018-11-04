package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.StoreName;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UsersGlobalStoreUpdater implements Processor<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UsersGlobalStoreUpdater.class);

    private KeyValueStore<String, String> usersGlobalStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        usersGlobalStore = (KeyValueStore<String, String>) context.getStateStore(StoreName.USERS_GLOBAL_STORE);
    }

    @Override
    public void process(String key, String value) {
        // REALLY IMPORTANT!!!
        // This method is NOT called when the app starts, and the global store is being built from entries already in the users topic!!!!
        // During startup records from the topic are directly pumped into the global store, without calling this method for each entry.
        // This method is only called for NEW records being inserted into the users topic AFTER the initial restore.
        // So don't do any "mutation" on key and value in this method!
        LOGGER.info("Inserting entry into global store: {}:{}", key, value);
        usersGlobalStore.put(key, value);
    }

    @Override
    public void close() {
        usersGlobalStore = null;
    }
}
