package com.nimbo.storage.hdfs;

import com.nimbo.config.HdfsProperties;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.stereotype.Component;

@Component
public class HdfsFileSystemFactory {

  public FileSystem create(HdfsProperties hdfsProperties) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    String uriStr = hdfsProperties.getUri();
    conf.set("fs.defaultFS", uriStr);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set("dfs.replication", "3");

    String nameservice = hdfsProperties.getNameservice();
    if (nameservice != null && !nameservice.isBlank()) {

      conf.set("dfs.nameservices", nameservice);
      Map<String, String> nodes = hdfsProperties.getNodes();
      if (nodes != null && !nodes.isEmpty()) {

        conf.set("dfs.ha.namenodes." + nameservice, String.join(",", nodes.keySet()));

        conf.setBoolean("dfs.client.failover.random.order", false);

        for (Map.Entry<String, String> entry : nodes.entrySet()) {

          conf.set(
              "dfs.namenode.rpc-address." + nameservice + "." + entry.getKey(), entry.getValue());
        }
      }

      conf.set(
          "dfs.client.failover.proxy.provider." + nameservice,
          "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }

    return FileSystem.newInstance(URI.create(uriStr), conf, "hadoop");
  }
}
