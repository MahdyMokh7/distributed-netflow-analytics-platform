package com.nimbo.extensions;


import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


public class KafkaExtension implements BeforeAllCallback, AfterAllCallback {

    private static KafkaContainer KAFKA_CONTAINER;
    private static String bootstrapServers;

    @Override
    public void beforeAll(ExtensionContext context) {

        KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
        KAFKA_CONTAINER.start();
        bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    }

    public static String getBootstrapServers(){
        return bootstrapServers;
    }

    @Override
    public void afterAll(ExtensionContext context) {

        if(KAFKA_CONTAINER != null){
            KAFKA_CONTAINER.stop();
        }
    }
}