package dk.ralu.examples.kafka.streams;

import dk.ralu.examples.kafka.streams.GlobalStoreTopology.Constant.StoreName;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

class JoinMailsAndUsersProcessor implements Processor<String, String> {

    private KeyValueStore<String, String> usersGlobalStore;
    private ProcessorContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        usersGlobalStore = (KeyValueStore<String, String>) context.getStateStore(StoreName.USERS_GLOBAL_STORE);
    }

    @Override
    public void process(String mail, String message) {
        String userName = usersGlobalStore.get(mail);
        String messageAndUserName = message + " from " + userName;
        context.forward(mail, messageAndUserName);
    }

    @Override
    public void close() {
        usersGlobalStore = null;
    }
}
