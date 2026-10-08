package com.nimbo.application;

import com.nimbo.config.KafkaProperties;
import com.nimbo.ingestion.KafkaNetFlowConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerManager {

  private final KafkaProperties kafkaProperties;
  private final RegisteredSchemaLoader registeredSchemaLoader;
  private final ConsumerInstanceFactory consumerInstanceFactory;

  private final List<KafkaNetFlowConsumer> consumers = new ArrayList<>();
  private ExecutorService consumerExecutor;

  public ConsumerManager(
      KafkaProperties kafkaProperties,
      RegisteredSchemaLoader registeredSchemaLoader,
      ConsumerInstanceFactory consumerInstanceFactory) {
    this.kafkaProperties = kafkaProperties;
    this.registeredSchemaLoader = registeredSchemaLoader;
    this.consumerInstanceFactory = consumerInstanceFactory;
  }

  @PostConstruct
  public void start() {
    int instanceCount = kafkaProperties.getConsumerInstances();
    List<Schema> registeredSchemas = registeredSchemaLoader.loadRegisteredSchemas();
    log.info("Starting {} consumer instances for topic: {}", instanceCount, kafkaProperties.getTopic());

    consumerExecutor = Executors.newFixedThreadPool(
        instanceCount,
        r -> {
          Thread t = new Thread(r);
          t.setName("consumer-instance-" + t.getId());
          t.setDaemon(true);
          return t;
        });

    for (int i = 0; i < instanceCount; i++) {

      int instanceId = i + 1;

      try {
        KafkaNetFlowConsumer consumer = consumerInstanceFactory.create(instanceId, registeredSchemas);

        consumers.add(consumer);
        consumerExecutor.submit(consumer::start);
        log.info("Started consumer instance #{}", i + 1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Thread interrupted while creating consumer instance #{}", i + 1, e);
        break;

      } catch (IOException | RuntimeException e) {
        log.error("Failed to create consumer instance #{}", i + 1, e);
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    log.info("Shutting down ConsumerManager...");

    for (KafkaNetFlowConsumer consumer : consumers) {
      consumer.shutdown();
    }

    if (consumerExecutor != null) {
      consumerExecutor.shutdown();
      try {
        if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          consumerExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        consumerExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    log.info("ConsumerManager shutdown complete");
  }
}
