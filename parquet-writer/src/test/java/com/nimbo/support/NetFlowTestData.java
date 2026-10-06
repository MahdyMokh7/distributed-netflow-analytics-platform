package com.nimbo.support;

import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ClassPathResource;

public final class NetFlowTestData {

  private final Schema sourceSchema;

  public NetFlowTestData() {
    try (InputStream input = new ClassPathResource("avro/NetFlowV2Record.avsc").getInputStream()) {
      sourceSchema = new Schema.Parser().parse(input);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to load NetFlow test schema", exception);
    }
  }

  public Schema sourceSchema() {
    return sourceSchema;
  }

  public GenericRecord validRecord() {
    return validRecord("192.168.10.25", "10.20.30.40", 12_000L);
  }

  public GenericRecord validRecord(String sourceIp, String destinationIp, long inboundBytes) {
    GenericData.Record record = new GenericData.Record(sourceSchema);
    record.put("IPV4_SRC_ADDR", sourceIp);
    record.put("IPV4_DST_ADDR", destinationIp);
    record.put("L4_SRC_PORT", 443);
    record.put("L4_DST_PORT", 51_000);
    record.put("PROTOCOL", 6);
    record.put("L7_PROTO", 91.0);
    record.put("IN_BYTES", inboundBytes);
    record.put("OUT_BYTES", inboundBytes / 2);
    record.put("IN_PKTS", 10L);
    record.put("OUT_PKTS", 8L);
    record.put("FLOW_DURATION_MILLISECONDS", 1_500L);
    record.put("TCP_FLAGS", 24);
    record.put("CLIENT_TCP_FLAGS", 2);
    record.put("SERVER_TCP_FLAGS", 18);
    record.put("DURATION_IN", 700L);
    record.put("DURATION_OUT", 800L);
    record.put("MIN_TTL", 60);
    record.put("MAX_TTL", 64);
    record.put("LONGEST_FLOW_PKT", 1_500);
    record.put("SHORTEST_FLOW_PKT", 64);
    record.put("MIN_IP_PKT_LEN", 64);
    record.put("MAX_IP_PKT_LEN", 1_500);
    record.put("SRC_TO_DST_SECOND_BYTES", 12_000L);
    record.put("DST_TO_SRC_SECOND_BYTES", 6_000L);
    record.put("RETRANSMITTED_IN_BYTES", 0L);
    record.put("RETRANSMITTED_IN_PKTS", 0L);
    record.put("RETRANSMITTED_OUT_BYTES", 0L);
    record.put("RETRANSMITTED_OUT_PKTS", 0L);
    record.put("SRC_TO_DST_AVG_THROUGHPUT", 96_000L);
    record.put("DST_TO_SRC_AVG_THROUGHPUT", 48_000L);
    record.put("NUM_PKTS_UP_TO_128_BYTES", 2L);
    record.put("NUM_PKTS_128_TO_256_BYTES", 2L);
    record.put("NUM_PKTS_256_TO_512_BYTES", 2L);
    record.put("NUM_PKTS_512_TO_1024_BYTES", 2L);
    record.put("NUM_PKTS_1024_TO_1514_BYTES", 2L);
    record.put("TCP_WIN_MAX_IN", 65_535);
    record.put("TCP_WIN_MAX_OUT", 65_535);
    record.put("ICMP_TYPE", 0);
    record.put("ICMP_IPV4_TYPE", 0);
    record.put("DNS_QUERY_ID", 0);
    record.put("DNS_QUERY_TYPE", 0);
    record.put("DNS_TTL_ANSWER", 0L);
    record.put("FTP_COMMAND_RET_CODE", 0);
    return record;
  }
}
