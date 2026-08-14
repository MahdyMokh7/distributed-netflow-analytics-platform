package com.nimbo.ingester.service;

import com.nimbo.ingester.model.ValidationResult;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;

@Service
public class NetflowValidationService {

    public ValidationResult validate(GenericRecord record) {

        StringBuilder errors = new StringBuilder();

        checkIpv4(record, "IPV4_SRC_ADDR", errors);
        checkIpv4(record, "IPV4_DST_ADDR", errors);

        checkRange(
                record,
                "L4_SRC_PORT",
                0,
                65535,
                errors
        );

        checkRange(
                record,
                "L4_DST_PORT",
                0,
                65535,
                errors
        );

        checkNonNegative(
                record,
                "IN_BYTES",
                errors
        );

        checkNonNegative(
                record,
                "OUT_BYTES",
                errors
        );

        checkNonNegative(
                record,
                "IN_PKTS",
                errors
        );

        checkNonNegative(
                record,
                "OUT_PKTS",
                errors
        );

        checkRange(
                record,
                "MIN_TTL",
                0,
                255,
                errors
        );

        checkRange(
                record,
                "MAX_TTL",
                0,
                255,
                errors
        );

        checkMinMax(
                record,
                "MIN_TTL",
                "MAX_TTL",
                errors
        );

        checkNonNegative(
                record,
                "LONGEST_FLOW_PKT",
                errors
        );

        checkNonNegative(
                record,
                "SHORTEST_FLOW_PKT",
                errors
        );

        checkMinMax(
                record,
                "SHORTEST_FLOW_PKT",
                "LONGEST_FLOW_PKT",
                errors
        );

        checkNonNegative(
                record,
                "MIN_IP_PKT_LEN",
                errors
        );

        checkNonNegative(
                record,
                "MAX_IP_PKT_LEN",
                errors
        );

        checkMinMax(
                record,
                "MIN_IP_PKT_LEN",
                "MAX_IP_PKT_LEN",
                errors
        );

        if (errors.isEmpty()) {
            return ValidationResult.ok();
        }

        return ValidationResult.invalid(
                errors.toString()
        );
    }

    private void checkNonNegative(
            GenericRecord record,
            String field,
            StringBuilder errors
    ) {

        Object value = record.get(field);

        if (value == null) {
            addError(
                    errors,
                    field + " is null"
            );
            return;
        }

        long number =
                ((Number) value).longValue();

        if (number < 0) {
            addError(
                    errors,
                    field
                            + " is negative: "
                            + number
            );
        }
    }

    private void checkRange(
            GenericRecord record,
            String field,
            long min,
            long max,
            StringBuilder errors
    ) {

        Object value = record.get(field);

        if (value == null) {
            addError(
                    errors,
                    field + " is null"
            );
            return;
        }

        long number =
                ((Number) value).longValue();

        if (number < min || number > max) {
            addError(
                    errors,
                    field
                            + " out of range: "
                            + number
            );
        }
    }

    private void checkMinMax(
            GenericRecord record,
            String minField,
            String maxField,
            StringBuilder errors
    ) {

        Object minObject =
                record.get(minField);

        Object maxObject =
                record.get(maxField);

        if (minObject == null || maxObject == null) {
            return;
        }

        long min =
                ((Number) minObject).longValue();

        long max =
                ((Number) maxObject).longValue();

        if (min > max) {
            addError(
                    errors,
                    minField
                            + " > "
                            + maxField
                            + " ("
                            + min
                            + " > "
                            + max
                            + ")"
            );
        }
    }

    private void checkIpv4(
            GenericRecord record,
            String field,
            StringBuilder errors
    ) {

        Object value = record.get(field);

        if (value == null) {
            addError(
                    errors,
                    field + " is null"
            );
            return;
        }

        String ip =
                value.toString();

        String[] parts =
                ip.split("\\.", -1);

        if (parts.length != 4) {
            addError(
                    errors,
                    field
                            + " invalid IPv4: "
                            + ip
            );
            return;
        }

        try {
            for (String part : parts) {

                if (part.isEmpty()) {
                    throw new NumberFormatException();
                }

                int number =
                        Integer.parseInt(part);

                if (number < 0 || number > 255) {
                    addError(
                            errors,
                            field
                                    + " invalid IPv4: "
                                    + ip
                    );
                    return;
                }
            }

        } catch (NumberFormatException e) {

            addError(
                    errors,
                    field
                            + " invalid IPv4: "
                            + ip
            );
        }
    }

    private void addError(
            StringBuilder errors,
            String error
    ) {

        if (errors.length() > 0) {
            errors.append(" | ");
        }

        errors.append(error);
    }
}