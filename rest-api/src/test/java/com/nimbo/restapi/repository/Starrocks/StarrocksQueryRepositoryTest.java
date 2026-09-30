package com.nimbo.restapi.repository.Starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StarrocksQueryRepositoryTest {

  @Test
  void trafficUsesSparkSourceIpHourlyCubeDefinition() {
    String sql = StarrocksQueryRepository.trafficSql("nimbo.netflow");

    assertThat(sql)
        .contains("ipv4_src_addr = :ip")
        .contains("GROUP BY ipv4_src_addr, window_start")
        .contains("COUNT(*) AS record_count")
        .doesNotContain("ipv4_dst_addr = :ip");
  }

  @Test
  void bandwidthUsesBothDirectionalPerspectivesIncludingSelfFlows() {
    String sql = StarrocksQueryRepository.bandwidthSql("nimbo.netflow");

    assertThat(sql)
        .contains("UNION ALL")
        .contains("in_bytes AS inbound_bytes")
        .contains("out_bytes AS inbound_bytes")
        .contains("GROUP BY ip, window_start")
        .doesNotContain("ipv4_dst_addr <>");
  }

  @Test
  void connectionsUseSparkSourceDestinationHourlyCubeDefinition() {
    String sql = StarrocksQueryRepository.connectionSql("nimbo.netflow");
    String countSql = StarrocksQueryRepository.connectionCountSql("nimbo.netflow");

    assertThat(sql)
        .contains("GROUP BY ipv4_src_addr, ipv4_dst_addr, window_start")
        .contains("SUM(in_bytes)")
        .contains("SUM(out_bytes)")
        .doesNotContain("l4_src_port", "l4_dst_port", "kafka_offset");
    assertThat(countSql).contains("GROUP BY ipv4_src_addr, ipv4_dst_addr, window_start");
  }

  @Test
  void protocolUsesL7ProtocolAndHourlyWindows() {
    String sql = StarrocksQueryRepository.protocolSql("nimbo.netflow");

    assertThat(sql)
        .contains("l7_proto")
        .contains("GROUP BY l7_proto, window_start")
        .contains("COUNT(*) AS connection_count")
        .doesNotContain("GROUP BY protocol");
  }
}
