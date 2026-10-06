package com.nimbo.support;

import java.net.URI;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public final class HdfsContainerExtension implements BeforeAllCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(HdfsContainerExtension.class);
  private static final Object HDFS_KEY = new Object();

  private SharedHdfs hdfs;

  @Override
  public void beforeAll(ExtensionContext context) {
    hdfs =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(HDFS_KEY, ignored -> SharedHdfsHolder.INSTANCE, SharedHdfs.class);
  }

  public String uri() {
    return "hdfs://" + hdfs().container.getHost() + ":" + hdfs().container.getMappedPort(8020);
  }

  public Configuration configuration() {
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", uri());
    configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    configuration.set("dfs.replication", "1");
    return configuration;
  }

  public FileSystem fileSystem() {
    try {
      Configuration configuration = configuration();
      return FileSystem.newInstance(URI.create(uri()), configuration, "hadoop");
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create HDFS test client", e);
    }
  }

  private SharedHdfs hdfs() {
    if (hdfs == null) {
      hdfs = SharedHdfsHolder.INSTANCE;
    }
    return hdfs;
  }

  private static final class SharedHdfsHolder {
    private static final SharedHdfs INSTANCE = new SharedHdfs();
  }

  private static final class SharedHdfs implements ExtensionContext.Store.CloseableResource {
    private final GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse("apache/hadoop:3.3.6"))
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("hadoop/core-site.xml"),
                "/opt/hadoop/etc/hadoop/core-site.xml")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("hadoop/hdfs-site.xml"),
                "/opt/hadoop/etc/hadoop/hdfs-site.xml")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("hadoop/start-hdfs.sh"),
                "/opt/hadoop/start-hdfs.sh")
            .withExposedPorts(8020, 9870, 9866)
            .withCommand("bash", "/opt/hadoop/start-hdfs.sh")
            .waitingFor(
                Wait.forLogMessage(".*HDFS_READY.*", 1).withStartupTimeout(Duration.ofMinutes(2)));

    private SharedHdfs() {
      container.start();
    }

    @Override
    public void close() {
      container.stop();
    }
  }
}
