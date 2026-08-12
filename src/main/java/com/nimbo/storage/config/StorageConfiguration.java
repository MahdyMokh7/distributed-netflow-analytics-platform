package com.nimbo.storage.config;

import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.BatchPolicy;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

@org.springframework.context.annotation.Configuration
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

    @Bean
    public BatchAccumulator batchAccumulator(BatchPolicy batchPolicy) {
        return new BatchAccumulator(batchPolicy);
    }

    @Bean
    public FileSystem fileSystem(
            HdfsProperties hdfsProperties
    ) throws IOException, InterruptedException {

        org.apache.hadoop.conf.Configuration conf =
                new org.apache.hadoop.conf.Configuration();

        conf.set("fs.defaultFS", hdfsProperties.getUri());

        conf.set("dfs.nameservices", "team1");

        conf.set(
                "dfs.ha.namenodes.team1",
                "team1-node1,team1-node2,team1-node3"
        );

        Map<String, String> nodes = hdfsProperties.getNodes();

        if (nodes != null) {
            conf.set(
                    "dfs.namenode.rpc-address.team1.team1-node1",
                    nodes.getOrDefault("node1", "10.10.61.51:8020")
            );

            conf.set(
                    "dfs.namenode.rpc-address.team1.team1-node2",
                    nodes.getOrDefault("node2", "10.10.61.52:8020")
            );

            conf.set(
                    "dfs.namenode.rpc-address.team1.team1-node3",
                    nodes.getOrDefault("node3", "10.10.61.53:8020")
            );
        } else {
            conf.set(
                    "dfs.namenode.rpc-address.team1.team1-node1",
                    "10.10.61.51:8020"
            );

            conf.set(
                    "dfs.namenode.rpc-address.team1.team1-node2",
                    "10.10.61.52:8020"
            );

            conf.set(
                    "dfs.namenode.rpc-address.team1.team1-node3",
                    "10.10.61.53:8020"
            );
        }

        conf.set(
                "dfs.client.failover.proxy.provider.team1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );

        conf.set(
                "fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );

        conf.set(
                "fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        return FileSystem.get(
                URI.create(hdfsProperties.getUri()),
                conf,
                hdfsProperties.getUser()
        );
    }
}