package com.nimbo.analyticsquery.schema;

import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.DIMENSION;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.IDENTIFIER;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.METADATA;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.METRIC;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.TIME;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class PostgresSemanticCatalogSource implements SemanticCatalogSource {

  private static final List<SemanticTable> TABLES =
      List.of(traffic(), connections(), bandwidth(), protocols());

  @Override
  public AnalysisDataSource dataSource() {
    return AnalysisDataSource.POSTGRESQL;
  }

  @Override
  public Collection<SemanticTable> tables() {
    return TABLES;
  }

  private static SemanticTable traffic() {
    return new SemanticTable(
        "traffic_aggregates",
        AnalysisDataSource.POSTGRESQL,
        "Traffic volume aggregated from NetFlow records by source IP and processing-time window.",
        "source IP x processing-time window x batch",
        "window_start",
        Duration.ofHours(1),
        List.of(
            semanticColumn(
                "src_ip", "INET", "Source IP address.", SemanticFieldNames.SOURCE_IP, DIMENSION),
            column("window_start", "TIMESTAMP", "Start of the aggregation window.", TIME),
            column("window_end", "TIMESTAMP", "End of the aggregation window.", TIME),
            semanticColumn(
                "in_bytes", "BIGINT", "Inbound bytes.", SemanticFieldNames.IN_BYTES, METRIC),
            semanticColumn(
                "out_bytes", "BIGINT", "Outbound bytes.", SemanticFieldNames.OUT_BYTES, METRIC),
            semanticColumn(
                "in_packets", "BIGINT", "Inbound packets.", SemanticFieldNames.IN_PACKETS, METRIC),
            semanticColumn(
                "out_packets",
                "BIGINT",
                "Outbound packets.",
                SemanticFieldNames.OUT_PACKETS,
                METRIC),
            semanticColumn(
                "record_count",
                "BIGINT",
                "Number of NetFlow records represented.",
                SemanticFieldNames.RECORD_COUNT,
                METRIC),
            column("batch_id", "UUID", "Batch that produced this aggregate.", METADATA),
            nullableColumn("ingested_at", "TIMESTAMP", "Database ingestion timestamp.", METADATA)),
        List.of("Derived from the same NetFlow stream as the other aggregate cubes."),
        List.of(
            "Traffic volume by source IP and time.",
            "Source-IP ranking by bytes, packets or record count.",
            "Total bytes or packets derived from inbound and outbound values."),
        List.of(
            "Destination-IP analysis.",
            "Port analysis.",
            "Protocol analysis.",
            "Raw individual-flow analysis."));
  }

  private static SemanticTable connections() {
    return new SemanticTable(
        "connection_aggregates",
        AnalysisDataSource.POSTGRESQL,
        "Traffic aggregated between source and destination IP pairs.",
        "source IP x destination IP x processing-time window x batch",
        "window_start",
        Duration.ofHours(1),
        List.of(
            column(
                "id",
                "BYTEA",
                "SHA-256 digest identifying the source/destination/window aggregate.",
                IDENTIFIER),
            semanticColumn(
                "src_ip", "INET", "Source IP address.", SemanticFieldNames.SOURCE_IP, DIMENSION),
            semanticColumn(
                "dst_ip",
                "INET",
                "Destination IP address.",
                SemanticFieldNames.DESTINATION_IP,
                DIMENSION),
            column("window_start", "TIMESTAMP", "Start of the aggregation window.", TIME),
            column("window_end", "TIMESTAMP", "End of the aggregation window.", TIME),
            semanticColumn(
                "in_bytes", "BIGINT", "Inbound bytes.", SemanticFieldNames.IN_BYTES, METRIC),
            semanticColumn(
                "out_bytes", "BIGINT", "Outbound bytes.", SemanticFieldNames.OUT_BYTES, METRIC),
            semanticColumn(
                "in_packets", "BIGINT", "Inbound packets.", SemanticFieldNames.IN_PACKETS, METRIC),
            semanticColumn(
                "out_packets",
                "BIGINT",
                "Outbound packets.",
                SemanticFieldNames.OUT_PACKETS,
                METRIC),
            column("batch_id", "UUID", "Batch that produced this aggregate.", METADATA),
            nullableColumn("ingested_at", "TIMESTAMP", "Database ingestion timestamp.", METADATA)),
        List.of(
            "Source and destination IP dimensions correspond to "
                + "the same IP concepts used by the other NetFlow tables."),
        List.of(
            "Traffic between source and destination IP pairs.",
            "Source or destination IP ranking.",
            "Pair-level byte and packet analysis."),
        List.of(
            "Source or destination port analysis.",
            "Layer-4 protocol analysis.",
            "Individual raw-flow analysis."));
  }

  private static SemanticTable bandwidth() {
    return new SemanticTable(
        "ip_bandwidth_aggregates",
        AnalysisDataSource.POSTGRESQL,
        "IP-centric bandwidth statistics combining source and destination perspectives.",
        "IP address x processing-time window x batch",
        "window_start",
        Duration.ofHours(1),
        List.of(
            semanticColumn(
                "ip",
                "INET",
                "IP address observed from the unified source/destination perspective.",
                SemanticFieldNames.IP_ADDRESS,
                DIMENSION),
            column("window_start", "TIMESTAMP", "Start of the aggregation window.", TIME),
            column("window_end", "TIMESTAMP", "End of the aggregation window.", TIME),
            semanticColumn(
                "inbound_bytes",
                "BIGINT",
                "Bytes received by the IP.",
                SemanticFieldNames.IN_BYTES,
                METRIC),
            semanticColumn(
                "outbound_bytes",
                "BIGINT",
                "Bytes sent by the IP.",
                SemanticFieldNames.OUT_BYTES,
                METRIC),
            semanticColumn(
                "inbound_packets",
                "BIGINT",
                "Packets received by the IP.",
                SemanticFieldNames.IN_PACKETS,
                METRIC),
            semanticColumn(
                "outbound_packets",
                "BIGINT",
                "Packets sent by the IP.",
                SemanticFieldNames.OUT_PACKETS,
                METRIC),
            column("batch_id", "UUID", "Batch that produced this aggregate.", METADATA),
            nullableColumn("ingested_at", "TIMESTAMP", "Database ingestion timestamp.", METADATA)),
        List.of(
            "Represents the same IP addresses as traffic and "
                + "connection aggregates but from an IP-centric perspective."),
        List.of(
            "Inbound and outbound bandwidth by IP.",
            "Top or bottom IPs by bandwidth or packet volume.",
            "Total bandwidth derived from inbound and outbound values."),
        List.of(
            "Source-to-destination pair analysis.",
            "Port or protocol analysis.",
            "Individual raw flows."));
  }

  private static SemanticTable protocols() {
    return new SemanticTable(
        "protocol_usage_aggregates",
        AnalysisDataSource.POSTGRESQL,
        "Layer-7 protocol usage aggregated by processing-time window.",
        "layer-7 protocol x processing-time window x batch",
        "window_start",
        Duration.ofHours(1),
        List.of(
            semanticColumn(
                "l7_protocol",
                "DOUBLE PRECISION",
                "Layer-7 protocol identifier.",
                SemanticFieldNames.L7_PROTOCOL,
                DIMENSION),
            column("window_start", "TIMESTAMP", "Start of the aggregation window.", TIME),
            column("window_end", "TIMESTAMP", "End of the aggregation window.", TIME),
            semanticColumn(
                "in_bytes", "BIGINT", "Inbound bytes.", SemanticFieldNames.IN_BYTES, METRIC),
            semanticColumn(
                "out_bytes", "BIGINT", "Outbound bytes.", SemanticFieldNames.OUT_BYTES, METRIC),
            semanticColumn(
                "in_packets", "BIGINT", "Inbound packets.", SemanticFieldNames.IN_PACKETS, METRIC),
            semanticColumn(
                "out_packets",
                "BIGINT",
                "Outbound packets.",
                SemanticFieldNames.OUT_PACKETS,
                METRIC),
            semanticColumn(
                "connection_count",
                "BIGINT",
                "Number of represented connections.",
                SemanticFieldNames.CONNECTION_COUNT,
                METRIC),
            column("batch_id", "UUID", "Batch that produced this aggregate.", METADATA),
            nullableColumn("ingested_at", "TIMESTAMP", "Database ingestion timestamp.", METADATA)),
        List.of(
            "Protocol identifiers originate from the same NetFlow input used by the other analytics"
                + " cubes."),
        List.of(
            "Protocol usage over time.", "Protocol ranking by bytes, packets or connection count."),
        List.of(
            "IP-specific protocol analysis.", "Port analysis.", "Individual raw-flow inspection."));
  }

  private static SemanticColumn column(
      String name, String type, String meaning, SemanticColumn.Role... roles) {

    return new SemanticColumn(name, type, meaning, java.util.Set.of(roles), false);
  }

  private static SemanticColumn nullableColumn(
      String name, String type, String meaning, SemanticColumn.Role... roles) {

    return new SemanticColumn(name, type, meaning, java.util.Set.of(roles), true);
  }

  private static SemanticColumn semanticColumn(
      String name,
      String type,
      String meaning,
      String semanticField,
      SemanticColumn.Role... roles) {

    Set<SemanticColumn.Role> roleSet = Set.of(roles);

    Set<Aggregation> aggregations =
        roleSet.contains(DIMENSION) ? Set.of(Aggregation.COUNT_DISTINCT) : Set.of(Aggregation.SUM);

    return new SemanticColumn(name, type, meaning, roleSet, false, semanticField, aggregations);
  }
}
