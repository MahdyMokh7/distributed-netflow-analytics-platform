package com.nimbo.starrockswriter.support;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

public final class TestContextInitializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        TestExtension.Infrastructure infrastructure = TestExtension.infrastructure();
        infrastructure.prepare();

        TestKafka kafka = infrastructure.kafka();
        TestStarRocks starRocks = infrastructure.starRocks();
        TestSchemaRegistry schemaRegistry = infrastructure.schemaRegistry();
        TestPropertyValues.of(
                        "spring.datasource.url=" + starRocks.jdbcUrl(),
                        "spring.datasource.username=root",
                        "spring.datasource.password=",
                        "starrockswriter.kafka.bootstrap-servers=" + kafka.bootstrapServers(),
                        "starrockswriter.kafka.topic=" + kafka.topic(),
                        "starrockswriter.kafka.consumer-group=" + kafka.consumerGroup(),
                        "starrockswriter.kafka.client-id=starrockswriter-integration-test",
                        "starrockswriter.kafka.auto-offset-reset=earliest",
                        "starrockswriter.kafka.max-poll-records=10",
                        "starrockswriter.kafka.poll-timeout-ms=100",
                        "starrockswriter.kafka.max-partition-fetch-bytes=1048576",
                        "starrockswriter.kafka.consumer-count=2",
                        "starrockswriter.schema-registry.url=" + schemaRegistry.url(),
                        "starrockswriter.starrocks.stream-load-backends[0]=http://" + starRocks.host() + ":"
                                + starRocks.beHttpPort(),
                        "starrockswriter.starrocks.stream-load-backends[1]=http://" + starRocks.host() + ":"
                                + starRocks.beHttpPort() + "/",
                        "starrockswriter.starrocks.database=" + starRocks.database(),
                        "starrockswriter.starrocks.table=" + starRocks.table(),
                        "starrockswriter.starrocks.username=root",
                        "starrockswriter.starrocks.password=",
                        "starrockswriter.starrocks.merge-commit-enabled=false",
                        "starrockswriter.starrocks.merge-commit-async=false",
                        "starrockswriter.starrocks.retry-backoff-ms=100",
                        "starrockswriter.starrocks.merge-commit-interval-ms=100",
                        "starrockswriter.starrocks.merge-commit-parallel=1",
                        "starrockswriter.starrocks.max-batch-size-rows=2",
                        "starrockswriter.starrocks.max-batch-time-ms=100",
                        "starrockswriter.starrocks.max-in-flight-requests=2")
                .applyTo(context.getEnvironment());
    }
}
