package com.nimbo.extensions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static java.time.Duration.ofMinutes;

public class HdfsExtension implements BeforeAllCallback, AfterAllCallback {
    private static GenericContainer<?> HDFS_CONTAINER; private static String hdfsUri;

    @Override
    public void beforeAll(ExtensionContext context) {
        HDFS_CONTAINER = new GenericContainer<>(DockerImageName.parse("apache/hadoop:3.3.6"))
                .withCopyFileToContainer(forClasspathResource("hadoop/core-site.xml"),
                        "/opt/hadoop/etc/hadoop/core-site.xml")
                .withCopyFileToContainer(forClasspathResource("hadoop/hdfs-site.xml"),
                        "/opt/hadoop/etc/hadoop/hdfs-site.xml")
                .withCopyFileToContainer(forClasspathResource("hadoop/start-hdfs.sh"),
                        "/opt/hadoop/start-hdfs.sh")
                .withExposedPorts(8020, 9870, 9866)
                .withCommand("bash", "/opt/hadoop/start-hdfs.sh")
                .waitingFor(forLogMessage(".*HDFS_READY.*", 1).withStartupTimeout(ofMinutes(2)));
        HDFS_CONTAINER.start();
        hdfsUri = "hdfs://" + HDFS_CONTAINER.getHost() + ":" + HDFS_CONTAINER.getMappedPort(8020);
    }

    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("dfs.replication", "1");
        return configuration;
    }

    public static String getHdfsUri(){
        return hdfsUri;
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (HDFS_CONTAINER != null) {
            HDFS_CONTAINER.stop();
        }
    }
}