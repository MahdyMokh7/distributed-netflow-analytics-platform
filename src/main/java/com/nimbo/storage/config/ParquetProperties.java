package com.nimbo.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "storage.parquet")
public class ParquetProperties {

    private String compressionCodec;
    private int blockSizeBytes;
    private int pageSizeBytes;
    private boolean enableDictionary;

    public String getCompressionCodec() { return compressionCodec; }
    public void setCompressionCodec(String compressionCodec) { this.compressionCodec = compressionCodec; }

    public int getBlockSizeBytes() { return blockSizeBytes; }
    public void setBlockSizeBytes(int blockSizeBytes) { this.blockSizeBytes = blockSizeBytes; }

    public int getPageSizeBytes() { return pageSizeBytes; }
    public void setPageSizeBytes(int pageSizeBytes) { this.pageSizeBytes = pageSizeBytes; }

    public boolean isEnableDictionary() { return enableDictionary; }
    public void setEnableDictionary(boolean enableDictionary) { this.enableDictionary = enableDictionary; }
}