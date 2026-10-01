package com.nimbo.analyticsquery.client.restapi;

public enum TrafficSortField {
    SOURCE_IP("srcIp"),
    WINDOW_START("windowStart"),
    WINDOW_END("windowEnd"),
    IN_BYTES("inBytes"),
    OUT_BYTES("outBytes"),
    IN_PACKETS("inPackets"),
    OUT_PACKETS("outPackets"),
    RECORD_COUNT("recordCount"),
    INGESTED_AT("ingestedAt");

    private final String parameterValue;

    TrafficSortField(String parameterValue) {
        this.parameterValue = parameterValue;
    }

    public String parameterValue() {
        return parameterValue;
    }
}
