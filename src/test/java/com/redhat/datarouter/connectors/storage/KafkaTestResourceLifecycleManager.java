package com.redhat.datarouter.connectors.storage;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;

public class KafkaTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger log = Logger.getLogger(Import.class.getName());


    @Override
    public Map<String, String> start() {
        Map<String, String> env = new HashMap<>();
        Map<String, String> storage = InMemoryConnector.switchIncomingChannelsToInMemory("storage-sink");
        Map<String, String> storageUpload = InMemoryConnector.switchOutgoingChannelsToInMemory("storage-upload-done");

        env.putAll(storage);
        env.putAll(storageUpload);


        System.out.println("Kafka inmemory channels are: ");
        env.forEach((k, v) -> System.out.println(k + "=" + v));

        log.info("Mock Kafka topic");
        return env;
    }

    @Override
    public void stop() {
        InMemoryConnector.clear();
    }

}
