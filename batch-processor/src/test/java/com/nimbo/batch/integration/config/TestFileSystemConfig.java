package com.nimbo.batch.integration.config;

import com.nimbo.batch.config.HdfsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.net.URI;

@TestConfiguration
public class TestFileSystemConfig {

    private final HdfsConfig hdfsConfig;

    public TestFileSystemConfig(HdfsConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }

    @Bean
    @Primary
    public FileSystem testFileSystem() throws IOException {
        Configuration conf = new Configuration();
        String uri = hdfsConfig.getUri();
        if (uri == null || uri.isEmpty()) {
            uri = "file://" + System.getProperty("java.io.tmpdir") + "/nimbo-test/parquet";
        }

        if (!uri.endsWith("/")) {
            uri = uri + "/";
        }

        conf.set("fs.defaultFS", uri);
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.hdfs.impl.disable.cache", "true");
        conf.set("fs.file.impl.disable.cache", "true");

        System.out.println("TestFileSystemConfig: Using URI: " + uri);

        return FileSystem.get(URI.create(uri), conf);
    }
}