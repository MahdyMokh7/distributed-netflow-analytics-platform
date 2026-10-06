package com.nimbo.analyticsquery.schema;

import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.DIMENSION;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.METADATA;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.METRIC;
import static com.nimbo.analyticsquery.schema.SemanticColumn.Role.TIME;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

final class StarRocksSemanticMetadata {

  private static final Set<Aggregation> DIMENSION_AGGREGATIONS =
      Set.of(Aggregation.COUNT, Aggregation.COUNT_DISTINCT);

  private static final Set<Aggregation> ADDITIVE_AGGREGATIONS =
      Set.of(Aggregation.NONE, Aggregation.SUM, Aggregation.AVG, Aggregation.MIN, Aggregation.MAX);

  private static final Set<Aggregation> MEASUREMENT_AGGREGATIONS =
      Set.of(Aggregation.NONE, Aggregation.AVG, Aggregation.MIN, Aggregation.MAX);

  private static final Map<String, String> SEMANTIC_FIELD_OVERRIDES =
      Map.ofEntries(
          Map.entry("ipv4_src_addr", SemanticFieldNames.SOURCE_IP),
          Map.entry("ipv4_dst_addr", SemanticFieldNames.DESTINATION_IP),
          Map.entry("l7_proto", SemanticFieldNames.L7_PROTOCOL),
          Map.entry("in_bytes", SemanticFieldNames.IN_BYTES),
          Map.entry("out_bytes", SemanticFieldNames.OUT_BYTES),
          Map.entry("in_pkts", SemanticFieldNames.IN_PACKETS),
          Map.entry("out_pkts", SemanticFieldNames.OUT_PACKETS));

  private static final Map<String, String> MEANING_OVERRIDES =
      Map.of(
          "ipv4_src_addr", "Source IPv4 address.",
          "ipv4_dst_addr", "Destination IPv4 address.",
          "l7_proto", "Layer-7 protocol identifier.",
          "process_time", "Kafka record timestamp in epoch milliseconds.");

  private static final Set<String> STRING_TYPES = Set.of("char", "varchar", "string");

  private StarRocksSemanticMetadata() {}

  static Optional<ColumnMetadata> metadata(String columnName, String dataType, String comment) {

    String name = columnName.toLowerCase(Locale.ROOT);
    String normalizedType = dataType.toLowerCase(Locale.ROOT);

    Optional<Set<SemanticColumn.Role>> roles = inferRoles(name, normalizedType);

    if (roles.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new ColumnMetadata(
            meaning(name, comment),
            roles.get(),
            semanticField(name, roles.get()),
            allowedAggregations(name, roles.get())));
  }

  private static Optional<Set<SemanticColumn.Role>> inferRoles(String name, String dataType) {

    if (name.equals("process_time") || name.endsWith("_timestamp")) {
      return Optional.of(Set.of(TIME));
    }

    if (name.startsWith("kafka_")) {
      return Optional.of(Set.of(METADATA));
    }

    if (isDimension(name) || STRING_TYPES.contains(dataType)) {
      return Optional.of(Set.of(DIMENSION));
    }

    if (isMetric(name)) {
      return Optional.of(Set.of(METRIC));
    }

    return Optional.empty();
  }

  private static boolean isDimension(String name) {
    return name.contains("addr")
        || name.endsWith("_port")
        || name.equals("protocol")
        || name.contains("_proto")
        || name.endsWith("_flags")
        || name.endsWith("_type")
        || name.endsWith("_id")
        || name.endsWith("_code");
  }

  private static boolean isMetric(String name) {
    return name.contains("bytes")
        || name.contains("pkts")
        || name.contains("packets")
        || name.contains("duration")
        || name.contains("throughput")
        || name.contains("ttl")
        || name.contains("pkt_len")
        || name.contains("flow_pkt")
        || name.contains("tcp_win");
  }

  private static String meaning(String name, String comment) {
    String override = MEANING_OVERRIDES.get(name);

    if (override != null) {
      return override;
    }

    if (comment != null && !comment.isBlank()) {
      return comment.strip();
    }

    String words =
        Arrays.stream(name.split("_"))
            .filter(part -> !part.isBlank())
            .collect(Collectors.joining(" "));

    return Character.toUpperCase(words.charAt(0)) + words.substring(1) + ".";
  }

  private static String semanticField(String name, Set<SemanticColumn.Role> roles) {

    if (!roles.contains(DIMENSION) && !roles.contains(METRIC)) {
      return null;
    }

    return SEMANTIC_FIELD_OVERRIDES.getOrDefault(name, name.toUpperCase(Locale.ROOT));
  }

  private static Set<Aggregation> allowedAggregations(String name, Set<SemanticColumn.Role> roles) {

    if (roles.contains(DIMENSION)) {
      return DIMENSION_AGGREGATIONS;
    }

    if (!roles.contains(METRIC)) {
      return Set.of();
    }

    return isMeasurement(name) ? MEASUREMENT_AGGREGATIONS : ADDITIVE_AGGREGATIONS;
  }

  private static boolean isMeasurement(String name) {
    return name.contains("duration")
        || name.contains("throughput")
        || name.contains("ttl")
        || name.contains("second_bytes")
        || name.contains("pkt_len")
        || name.contains("flow_pkt")
        || name.contains("tcp_win");
  }

  record ColumnMetadata(
      String meaning,
      Set<SemanticColumn.Role> roles,
      String semanticField,
      Set<Aggregation> allowedAggregations) {}
}
