package com.nimbo.batch.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;

@Configuration
public class HdfsFileSystemConfig {

    private final HdfsConfig hdfsConfig;

    public HdfsFileSystemConfig(HdfsConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }

    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        configuration.set("fs.defaultFS", hdfsConfig.getUri());
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        configuration.set("dfs.replication", "1");

        return FileSystem.get(
                URI.create(hdfsConfig.getUri()),
                configuration
        );
    }
}