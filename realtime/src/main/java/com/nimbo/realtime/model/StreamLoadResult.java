package com.nimbo.realtime.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamLoadResult(

        @JsonProperty("TxnId")
        Long txnId,

        @JsonProperty("Label")
        String label,

        @JsonProperty("Status")
        String status,

        @JsonProperty("Message")
        String message,

        @JsonProperty("NumberTotalRows")
        Long numberTotalRows,

        @JsonProperty("NumberLoadedRows")
        Long numberLoadedRows,

        @JsonProperty("NumberFilteredRows")
        Long numberFilteredRows,

        @JsonProperty("LoadBytes")
        Long loadBytes,

        @JsonProperty("BeginTxnTimeMs")
        Long beginTxnTimeMs,

        @JsonProperty("StreamLoadPutTimeMs")
        Long streamLoadPutTimeMs,

        @JsonProperty("ReadDataTimeMs")
        Long readDataTimeMs,

        @JsonProperty("WriteDataTimeMs")
        Long writeDataTimeMs,

        @JsonProperty("CommitAndPublishTimeMs")
        Long commitAndPublishTimeMs,

        @JsonProperty("LoadTimeMs")
        Long loadTimeMs
) {
    public boolean isSuccessful() {
        return "Success".equalsIgnoreCase(status);
    }
}
