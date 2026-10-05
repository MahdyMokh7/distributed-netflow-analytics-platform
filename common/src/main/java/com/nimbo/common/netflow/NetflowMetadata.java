package com.nimbo.common.netflow;

import java.util.List;

public final class NetflowMetadata {

  public static final String PROCESS_TIME = "PROCESS_TIME";
  public static final String KAFKA_TOPIC = "KAFKA_TOPIC";
  public static final String KAFKA_PARTITION = "KAFKA_PARTITION";
  public static final String KAFKA_OFFSET = "KAFKA_OFFSET";

  public static final String PROCESS_TIME_DOC = "Kafka record timestamp in UTC epoch milliseconds.";
  public static final String KAFKA_TOPIC_DOC = "Kafka source topic.";
  public static final String KAFKA_PARTITION_DOC = "Kafka source partition.";
  public static final String KAFKA_OFFSET_DOC = "Kafka source offset.";

  public static final List<String> RESERVED_FIELDS = List.of(
      PROCESS_TIME,
      KAFKA_TOPIC,
      KAFKA_PARTITION,
      KAFKA_OFFSET);

  private NetflowMetadata() {
  }
}
