package com.nimbo.realtime.test;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

public final class NimboTestContextInitializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        NimboTestExtension.Infrastructure infrastructure = NimboTestExtension.infrastructure();
        infrastructure.prepare();

        TestKafka kafka = infrastructure.kafka();
        TestStarRocks starRocks = infrastructure.starRocks();
        TestPropertyValues.of(
                        "spring.datasource.url=" + starRocks.jdbcUrl(),
                        "spring.datasource.username=root",
                        "spring.datasource.password=",
                        "realtime.kafka.bootstrap-servers=" + kafka.bootstrapServers(),
                        "realtime.kafka.topic=" + kafka.topic(),
                        "realtime.kafka.consumer-group=" + kafka.consumerGroup(),
                        "realtime.kafka.client-id=realtime-integration-test",
                        "realtime.kafka.auto-offset-reset=earliest",
                        "realtime.kafka.max-poll-records=10",
                        "realtime.kafka.poll-timeout-ms=100",
                        "realtime.kafka.max-partition-fetch-bytes=1048576",
                        "realtime.kafka.consumer-count=2",
                        "realtime.starrocks.stream-load-backends[0]=http://" + starRocks.host() + ":"
                                + starRocks.beHttpPort(),
                        "realtime.starrocks.stream-load-backends[1]=http://" + starRocks.host() + ":"
                                + starRocks.beHttpPort() + "/",
                        "realtime.starrocks.database=" + starRocks.database(),
                        "realtime.starrocks.table=" + starRocks.table(),
                        "realtime.starrocks.username=root",
                        "realtime.starrocks.password=",
                        "realtime.starrocks.merge-commit-enabled=false",
                        "realtime.starrocks.merge-commit-async=false",
                        "realtime.starrocks.retry-backoff-ms=100",
                        "realtime.starrocks.merge-commit-interval-ms=100",
                        "realtime.starrocks.merge-commit-parallel=1",
                        "realtime.starrocks.max-batch-size-rows=2",
                        "realtime.starrocks.max-batch-time-ms=100",
                        "realtime.starrocks.max-in-flight-requests=2")
                .applyTo(context.getEnvironment());
    }
}
