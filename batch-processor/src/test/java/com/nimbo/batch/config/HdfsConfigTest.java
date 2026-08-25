package com.nimbo.batch.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HdfsConfigTest {

    @Test
    void shouldConcatenateUriAndParquetPathIntoFullPath() {
        HdfsConfig config = new HdfsConfig();
        config.setUri("hdfs://10.10.61.53:8020");
        config.setParquetPath("/netflow/parquetFiles/");

        assertThat(config.getFullParquetPath())
                .isEqualTo("hdfs://10.10.61.53:8020/netflow/parquetFiles/");
    }

    @Test
    void shouldExposeRawUriAndPathSeparately() {
        HdfsConfig config = new HdfsConfig();
        config.setUri("hdfs://host:8020");
        config.setParquetPath("/data/");

        assertThat(config.getUri()).isEqualTo("hdfs://host:8020");
        assertThat(config.getParquetPath()).isEqualTo("/data/");
    }
}