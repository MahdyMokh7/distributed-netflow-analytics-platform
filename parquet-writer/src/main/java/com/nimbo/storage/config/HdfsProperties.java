package com.nimbo.storage.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Setter
@Getter
@ConfigurationProperties(prefix = "storage.hdfs")
public class HdfsProperties {

    private String uri;
    private String basePath;
    private String nameservice;
    private Map<String, String> nodes;

}