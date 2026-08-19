package com.nimbo.realtime.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record StarRocksRow(

        // Kafka metadata
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        long kafkaTimestamp,

        // NetFlow
        String ipv4SrcAddr,
        String ipv4DstAddr,

        int l4SrcPort,
        int l4DstPort,

        int protocol,
        double l7Proto,

        long inBytes,
        long outBytes,

        long inPkts,
        long outPkts,

        long flowDurationMilliseconds,

        int tcpFlags,
        int clientTcpFlags,
        int serverTcpFlags,

        long durationIn,
        long durationOut,

        int minTtl,
        int maxTtl,

        int longestFlowPkt,
        int shortestFlowPkt,

        int minIpPktLen,
        int maxIpPktLen,

        long srcToDstSecondBytes,
        long dstToSrcSecondBytes,

        long retransmittedInBytes,
        long retransmittedInPkts,

        long retransmittedOutBytes,
        long retransmittedOutPkts,

        long srcToDstAvgThroughput,
        long dstToSrcAvgThroughput,

        long numPktsUpTo128Bytes,
        long numPkts128To256Bytes,
        long numPkts256To512Bytes,
        long numPkts512To1024Bytes,
        long numPkts1024To1514Bytes,

        int tcpWinMaxIn,
        int tcpWinMaxOut,

        int icmpType,
        int icmpIpv4Type,

        int dnsQueryId,
        int dnsQueryType,
        long dnsTtlAnswer,

        int ftpCommandRetCode
) {
}