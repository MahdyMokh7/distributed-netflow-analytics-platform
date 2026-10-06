package com.nimbo.cubemaker.config;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

    return FileSystem.get(URI.create(hdfsConfig.getUri()), configuration);
  }
}
