package com.nimbo.analyticsquery.client.restapi;

public enum ConnectionSortField {
    ID("id"),
    SOURCE_IP("srcIp"),
    DESTINATION_IP("dstIp"),
    WINDOW_START("windowStart"),
    WINDOW_END("windowEnd"),
    IN_BYTES("inBytes"),
    OUT_BYTES("outBytes"),
    IN_PACKETS("inPackets"),
    OUT_PACKETS("outPackets"),
    INGESTED_AT("ingestedAt");

    private final String parameterValue;

    ConnectionSortField(String parameterValue) {
        this.parameterValue = parameterValue;
    }

    public String parameterValue() {
        return parameterValue;
    }
}
