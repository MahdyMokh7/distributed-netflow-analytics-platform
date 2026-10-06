package com.nimbo.backfill.model;

import java.math.BigDecimal;
import java.util.Objects;

public record BackfillMetrics(
    long rowCount,
    BigDecimal inBytes,
    BigDecimal outBytes,
    BigDecimal inPackets,
    BigDecimal outPackets,
    Long minimumTimestamp,
    Long maximumTimestamp) {

  public BackfillMetrics {
    if (rowCount < 0) {
      throw new IllegalArgumentException("rowCount must not be negative");
    }
    if ((minimumTimestamp == null) != (maximumTimestamp == null)) {
      throw new IllegalArgumentException(
          "minimumTimestamp and maximumTimestamp must both be null or non-null");
    }
  }

  public static BackfillMetrics empty() {
    return new BackfillMetrics(
        0, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, null, null);
  }

  public BackfillMetrics combine(BackfillMetrics other) {
    return new BackfillMetrics(
        Math.addExact(rowCount, other.rowCount),
        inBytes.add(other.inBytes),
        outBytes.add(other.outBytes),
        inPackets.add(other.inPackets),
        outPackets.add(other.outPackets),
        minimum(minimumTimestamp, other.minimumTimestamp),
        maximum(maximumTimestamp, other.maximumTimestamp));
  }

  public boolean sameDataAs(BackfillMetrics other) {
    return rowCount == other.rowCount
        && inBytes.compareTo(other.inBytes) == 0
        && outBytes.compareTo(other.outBytes) == 0
        && inPackets.compareTo(other.inPackets) == 0
        && outPackets.compareTo(other.outPackets) == 0
        && Objects.equals(minimumTimestamp, other.minimumTimestamp)
        && Objects.equals(maximumTimestamp, other.maximumTimestamp);
  }

  public String summary() {
    return "rows="
        + rowCount
        + ", inBytes="
        + inBytes.toPlainString()
        + ", outBytes="
        + outBytes.toPlainString()
        + ", inPackets="
        + inPackets.toPlainString()
        + ", outPackets="
        + outPackets.toPlainString()
        + ", minTimestamp="
        + minimumTimestamp
        + ", maxTimestamp="
        + maximumTimestamp;
  }

  private static Long minimum(Long first, Long second) {
    if (first == null) {
      return second;
    }
    if (second == null) {
      return first;
    }
    return Math.min(first, second);
  }

  private static Long maximum(Long first, Long second) {
    if (first == null) {
      return second;
    }
    if (second == null) {
      return first;
    }
    return Math.max(first, second);
  }
}
