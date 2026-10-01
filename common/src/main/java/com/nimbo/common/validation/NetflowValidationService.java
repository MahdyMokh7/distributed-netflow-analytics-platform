package com.nimbo.common.validation;

import org.apache.avro.generic.GenericRecord;

public class NetflowValidationService {

    private static final String FLOW_DURATION_MILLISECONDS = "FLOW_DURATION_MILLISECONDS";

    public ValidationResult validate(GenericRecord genericRecord) {
        if (genericRecord == null) {
            return ValidationResult.failure("Record is null");
        }

        StringBuilder errors = new StringBuilder();

        checkIpv4(genericRecord, "IPV4_SRC_ADDR", errors);
        checkIpv4(genericRecord, "IPV4_DST_ADDR", errors);

        checkRange(genericRecord, "L4_SRC_PORT", 0, 65_535, errors);
        checkRange(genericRecord, "L4_DST_PORT", 0, 65_535, errors);
        checkRange(genericRecord, "PROTOCOL", 0, 255, errors);

        checkRange(genericRecord, "MIN_TTL", 0, 255, errors);
        checkRange(genericRecord, "MAX_TTL", 0, 255, errors);

        checkRange(genericRecord, "MIN_IP_PKT_LEN", 0, 65_535, errors);
        checkRange(genericRecord, "MAX_IP_PKT_LEN", 0, 65_535, errors);

        checkRange(genericRecord, "ICMP_TYPE", 0, 65_535, errors);
        checkRange(genericRecord, "ICMP_IPV4_TYPE", 0, 255, errors);

        checkRange(genericRecord, "DNS_QUERY_ID", 0, 65_535, errors);
        checkRange(genericRecord, "DNS_QUERY_TYPE", 0, 65_535, errors);

        checkNonNegative(genericRecord, "IN_BYTES", errors);
        checkNonNegative(genericRecord, "OUT_BYTES", errors);
        checkNonNegative(genericRecord, "IN_PKTS", errors);
        checkNonNegative(genericRecord, "OUT_PKTS", errors);

        checkNonNegative(genericRecord, FLOW_DURATION_MILLISECONDS, errors);
        checkNonNegative(genericRecord, "DURATION_IN", errors);
        checkNonNegative(genericRecord, "DURATION_OUT", errors);

        checkNonNegative(genericRecord, "LONGEST_FLOW_PKT", errors);
        checkNonNegative(genericRecord, "SHORTEST_FLOW_PKT", errors);

        checkMinMax(genericRecord, "MIN_TTL", "MAX_TTL", errors);
        checkMinMax(genericRecord, "SHORTEST_FLOW_PKT", "LONGEST_FLOW_PKT", errors);
        checkMinMax(genericRecord, "MIN_IP_PKT_LEN", "MAX_IP_PKT_LEN", errors);
        checkMinMax(genericRecord, "DURATION_OUT", FLOW_DURATION_MILLISECONDS, errors);
        checkMinMax(genericRecord, "DURATION_IN", FLOW_DURATION_MILLISECONDS, errors);

        checkFiniteNonNegativeDouble(genericRecord, "L7_PROTO", errors);

        if (errors.isEmpty()) {
            return ValidationResult.success();
        }

        return ValidationResult.failure(errors.toString());
    }

    private void checkNonNegative(GenericRecord record, String field, StringBuilder errors) {
        Number value = number(record, field, errors);

        if (value != null && value.longValue() < 0) {
            addError(errors, field + " is negative: " + value);
        }
    }

    private void checkRange(GenericRecord record, String field, long min, long max, StringBuilder errors) {
        Number value = number(record, field, errors);

        if (value == null) {
            return;
        }

        long number = value.longValue();

        if (number < min || number > max) {
            addError(errors, field + " out of range [" + min + ", " + max + "]: " + number);
        }
    }

    private void checkMinMax(GenericRecord record, String minField, String maxField, StringBuilder errors) {
        if (!hasField(record, minField) || !hasField(record, maxField)) {
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

    private void checkFiniteNonNegativeDouble(GenericRecord record, String field, StringBuilder errors) {
        Number value = number(record, field, errors);

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

    private Number number(GenericRecord record, String field, StringBuilder errors) {
        if (!hasField(record, field)) {
            return null;
        }

        Object value = record.get(field);
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

    private void checkIpv4(GenericRecord record, String field, StringBuilder errors) {
        if (!hasField(record, field)) {
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
            addInvalidIpv4(errors, field, ip);
            return;
        }

        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3 || !part.chars().allMatch(Character::isDigit)) {
                addInvalidIpv4(errors, field, ip);
                return;
            }

            int number = Integer.parseInt(part);

            if (number > 255) {
                addInvalidIpv4(errors, field, ip);
                return;
            }
        }
    }

    private void addInvalidIpv4(StringBuilder errors, String field, String ip) {
        addError(errors, field + " invalid IPv4: " + ip);
    }

    private boolean hasField(GenericRecord record, String field) {
        return record.getSchema().getField(field) != null;
    }

    private void addError(StringBuilder errors, String error) {
        if (!errors.isEmpty()) {
            errors.append(" | ");
        }
        errors.append(error);
    }
}
