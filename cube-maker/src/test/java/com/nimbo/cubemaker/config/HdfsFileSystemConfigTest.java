package com.nimbo.cubemaker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

class HdfsFileSystemConfigTest {

  @Test
  void givenDefaultHadoopConfiguration_whenFileSystemIsCreated_thenReplicationIsNotOverridden()
      throws Exception {
    // Given: a filesystem using the default Hadoop replication setting
    HdfsConfig hdfsConfig = new HdfsConfig();
    hdfsConfig.setUri("file:///");
    HdfsFileSystemConfig config = new HdfsFileSystemConfig(hdfsConfig);

    // When: the application creates its filesystem client
    FileSystem fileSystem = config.fileSystem();

    // Then: the client does not force a replication factor
    try {
      assertThat(fileSystem.getConf().get("dfs.replication")).isNull();
    } finally {
      fileSystem.close();
    }
  }
}
