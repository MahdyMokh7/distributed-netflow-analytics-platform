package com.nimbo.analyticsquery.schema;

import java.util.Set;

public final class SemanticFieldNames {

  public static final String SOURCE_IP = "SOURCE_IP";
  public static final String DESTINATION_IP = "DESTINATION_IP";
  public static final String IP_ADDRESS = "IP_ADDRESS";
  public static final String L7_PROTOCOL = "L7_PROTOCOL";
  public static final String IN_BYTES = "IN_BYTES";
  public static final String OUT_BYTES = "OUT_BYTES";
  public static final String TOTAL_BYTES = "TOTAL_BYTES";
  public static final String IN_PACKETS = "IN_PACKETS";
  public static final String OUT_PACKETS = "OUT_PACKETS";
  public static final String TOTAL_PACKETS = "TOTAL_PACKETS";
  public static final String RECORD_COUNT = "RECORD_COUNT";
  public static final String CONNECTION_COUNT = "CONNECTION_COUNT";

  public static final Set<String> INBOUND_METRICS = Set.of(IN_BYTES, IN_PACKETS);
  public static final Set<String> OUTBOUND_METRICS = Set.of(OUT_BYTES, OUT_PACKETS);

  private SemanticFieldNames() {}
}
