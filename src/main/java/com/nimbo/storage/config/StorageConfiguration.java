package com.nimbo.storage.config;

import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.BatchPolicy;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

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

        String uriStr = hdfsProperties.getUri();
        conf.set("fs.defaultFS", uriStr);

        String nameservice = hdfsProperties.getNameservice();
        if (nameservice != null && !nameservice.isBlank()) {
            conf.set("dfs.nameservices", nameservice);

            Map<String, String> nodes = hdfsProperties.getNodes();
            if (nodes != null && !nodes.isEmpty()) {
                // Sets "team1-node1,team1-node2,team1-node3"
                String nodeAliases = String.join(",", nodes.keySet());
                conf.set("dfs.ha.namenodes." + nameservice, nodeAliases);

                // Registers IP:Port for each node
                for (Map.Entry<String, String> entry : nodes.entrySet()) {
                    conf.set(
                            "dfs.namenode.rpc-address." + nameservice + "." + entry.getKey(),
                            entry.getValue()
                    );
                }
            }

            conf.set(
                    "dfs.client.failover.proxy.provider." + nameservice,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            );
        }

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String user = (hdfsProperties.getUser() != null && !hdfsProperties.getUser().isBlank())
                ? hdfsProperties.getUser()
                : "root";

        return FileSystem.get(URI.create(uriStr), conf, user);
    }
}