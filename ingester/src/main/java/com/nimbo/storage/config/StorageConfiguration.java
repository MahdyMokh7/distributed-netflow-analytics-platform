package com.nimbo.storage.config;

import com.nimbo.storage.service.batching.BatchPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableConfigurationProperties({
        RollingProperties.class,
        HdfsProperties.class,
        ParquetProperties.class
})
public class StorageConfiguration {

    @Bean
    public BatchPolicy batchPolicy(RollingProperties properties) {
        return new BatchPolicy(
                properties.getMaxRecordCount(),
                properties.getMaxSizeBytes(),
                properties.getMaxAge()
        );
    }
}