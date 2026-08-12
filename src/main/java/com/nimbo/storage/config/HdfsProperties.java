package com.nimbo.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "storage.hdfs")
public class HdfsProperties {

    private String uri;
    private String user;
    private String basePath;
    private String nameservice;
    private Map<String, String> nodes;

    public String getUri() { return uri; }
    public void setUri(String uri) { this.uri = uri; }

    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }

    public String getBasePath() { return basePath; }
    public void setBasePath(String basePath) { this.basePath = basePath; }

    public String getNameservice() { return nameservice; }
    public void setNameservice(String nameservice) { this.nameservice = nameservice; }

    public Map<String, String> getNodes() { return nodes; }
    public void setNodes(Map<String, String> nodes) { this.nodes = nodes; }
}