package com.nimbo.common.validation;

import com.nimbo.common.avro.AvroSchemaSupport;
import org.apache.avro.generic.GenericRecord;

public class NetflowValidationService {

  private static final String FLOW_DURATION_MILLISECONDS = "FLOW_DURATION_MILLISECONDS";

  public ValidationResult validate(GenericRecord genericRecord) {
    if (genericRecord == null) {
      return ValidationResult.failure("Record is null");
    }

    StringBuilder errors = new StringBuilder();

    verifyIpv4(genericRecord, "IPV4_SRC_ADDR", errors);
    verifyIpv4(genericRecord, "IPV4_DST_ADDR", errors);

    verifyRange(genericRecord, "L4_SRC_PORT", 0, 65_535, errors);
    verifyRange(genericRecord, "L4_DST_PORT", 0, 65_535, errors);
    verifyRange(genericRecord, "PROTOCOL", 0, 255, errors);

    verifyRange(genericRecord, "MIN_TTL", 0, 255, errors);
    verifyRange(genericRecord, "MAX_TTL", 0, 255, errors);

    verifyRange(genericRecord, "MIN_IP_PKT_LEN", 0, 65_535, errors);
    verifyRange(genericRecord, "MAX_IP_PKT_LEN", 0, 65_535, errors);

    verifyRange(genericRecord, "ICMP_TYPE", 0, 65_535, errors);
    verifyRange(genericRecord, "ICMP_IPV4_TYPE", 0, 255, errors);

    verifyRange(genericRecord, "DNS_QUERY_ID", 0, 65_535, errors);
    verifyRange(genericRecord, "DNS_QUERY_TYPE", 0, 65_535, errors);

    verifyNonNegative(genericRecord, "IN_BYTES", errors);
    verifyNonNegative(genericRecord, "OUT_BYTES", errors);
    verifyNonNegative(genericRecord, "IN_PKTS", errors);
    verifyNonNegative(genericRecord, "OUT_PKTS", errors);

    verifyNonNegative(genericRecord, FLOW_DURATION_MILLISECONDS, errors);
    verifyNonNegative(genericRecord, "DURATION_IN", errors);
    verifyNonNegative(genericRecord, "DURATION_OUT", errors);

    verifyNonNegative(genericRecord, "LONGEST_FLOW_PKT", errors);
    verifyNonNegative(genericRecord, "SHORTEST_FLOW_PKT", errors);

    verifyMinMax(genericRecord, "MIN_TTL", "MAX_TTL", errors);
    verifyMinMax(genericRecord, "SHORTEST_FLOW_PKT", "LONGEST_FLOW_PKT", errors);
    verifyMinMax(genericRecord, "MIN_IP_PKT_LEN", "MAX_IP_PKT_LEN", errors);
    verifyMinMax(genericRecord, "DURATION_OUT", FLOW_DURATION_MILLISECONDS, errors);
    verifyMinMax(genericRecord, "DURATION_IN", FLOW_DURATION_MILLISECONDS, errors);

    verifyFiniteNonNegativeDouble(genericRecord, "L7_PROTO", errors);

    if (errors.isEmpty()) {
      return ValidationResult.success();
    }

    return ValidationResult.failure(errors.toString());
  }

  private void verifyNonNegative(GenericRecord genericRecord, String field, StringBuilder errors) {
    Number value = readNumber(genericRecord, field, errors);

    if (value != null && value.longValue() < 0) {
      addError(errors, field + " is negative: " + value);
    }
  }

  private void verifyRange(GenericRecord genericRecord, String field, long min, long max, StringBuilder errors) {
    Number value = readNumber(genericRecord, field, errors);

    if (value == null) {
      return;
    }

    long number = value.longValue();

    if (number < min || number > max) {
      addError(errors, field + " out of range [" + min + ", " + max + "]: " + number);
    }
  }

  private void verifyMinMax(GenericRecord record, String minField, String maxField, StringBuilder errors) {
    if (!containsField(record, minField) || !containsField(record, maxField)) {
      return;
    }

    Object minObject = record.get(minField);
    Object maxObject = record.get(maxField);

    if (!(minObject instanceof Number min) || !(maxObject instanceof Number max)) {
      return;
    }

    if (min.longValue() > max.longValue()) {
      addError(errors, minField + " > " + maxField + " (" + min + " > " + max + ")");
    }
  }

  private void verifyFiniteNonNegativeDouble(GenericRecord genericRecord, String field, StringBuilder errors) {
    Number value = readNumber(genericRecord, field, errors);

    if (value == null) {
      return;
    }

    double number = value.doubleValue();

    if (!Double.isFinite(number)) {
      addError(errors, field + " is not finite: " + number);
    } else if (number < 0) {
      addError(errors, field + " is negative: " + number);
    }
  }

  private Number readNumber(GenericRecord genericRecord, String field, StringBuilder errors) {
    if (!containsField(genericRecord, field)) {
      return null;
    }

    Object value = genericRecord.get(field);
    if (value == null) {
      addError(errors, field + " is null");
      return null;
    }

    if (!(value instanceof Number number)) {
      addError(errors, field + " is not numeric");
      return null;
    }
    return number;
  }

  private void verifyIpv4(GenericRecord record, String field, StringBuilder errors) {
    if (!containsField(record, field)) {
      return;
    }

    Object value = record.get(field);
    if (value == null) {
      addError(errors, field + " is null");
      return;
    }

    String ip = value.toString();
    String[] parts = ip.split("\\.", -1);

    if (parts.length != 4) {
      reportInvalidIpv4(errors, field, ip);
      return;
    }

    for (String part : parts) {
      if (part.isEmpty() || part.length() > 3 || !part.chars().allMatch(Character::isDigit)) {
        reportInvalidIpv4(errors, field, ip);
        return;
      }

      int number = Integer.parseInt(part);

      if (number > 255) {
        reportInvalidIpv4(errors, field, ip);
        return;
      }
    }
  }

  private void reportInvalidIpv4(StringBuilder errors, String field, String ip) {
    addError(errors, field + " invalid IPv4: " + ip);
  }

  private boolean containsField(GenericRecord genericRecord, String field) {
    return AvroSchemaSupport.hasField(genericRecord, field);
  }

  private void addError(StringBuilder errors, String error) {
    if (!errors.isEmpty()) {
      errors.append(" | ");
    }
    errors.append(error);
  }
}
