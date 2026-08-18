package com.nimbo.ingester.service;

import com.nimbo.ingester.model.ValidationResult;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;

@Service
public class NetflowValidationService {

    private static final String NULL_ERROR = " is null";
    private static final String INVALID_IPV4_ERROR = " invalid IPv4: ";

    public ValidationResult validate(GenericRecord genericRecord) {

        StringBuilder errors = new StringBuilder();

        checkIpv4(genericRecord, "IPV4_SRC_ADDR", errors);
        checkIpv4(genericRecord, "IPV4_DST_ADDR", errors);

        checkRange(
                genericRecord,
                "L4_SRC_PORT",
                0,
                65535,
                errors
        );

        checkRange(
                genericRecord,
                "L4_DST_PORT",
                0,
                65535,
                errors
        );

        checkNonNegative(
                genericRecord,
                "IN_BYTES",
                errors
        );

        checkNonNegative(
                genericRecord,
                "OUT_BYTES",
                errors
        );

        checkNonNegative(
                genericRecord,
                "IN_PKTS",
                errors
        );

        checkNonNegative(
                genericRecord,
                "OUT_PKTS",
                errors
        );

        checkRange(
                genericRecord,
                "MIN_TTL",
                0,
                255,
                errors
        );

        checkRange(
                genericRecord,
                "MAX_TTL",
                0,
                255,
                errors
        );

        checkMinMax(
                genericRecord,
                "MIN_TTL",
                "MAX_TTL",
                errors
        );

        checkNonNegative(
                genericRecord,
                "LONGEST_FLOW_PKT",
                errors
        );

        checkNonNegative(
                genericRecord,
                "SHORTEST_FLOW_PKT",
                errors
        );

        checkMinMax(
                genericRecord,
                "SHORTEST_FLOW_PKT",
                "LONGEST_FLOW_PKT",
                errors
        );

        checkNonNegative(
                genericRecord,
                "MIN_IP_PKT_LEN",
                errors
        );

        checkNonNegative(
                genericRecord,
                "MAX_IP_PKT_LEN",
                errors
        );

        checkMinMax(
                genericRecord,
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
            GenericRecord genericRecord,
            String field,
            StringBuilder errors
    ) {

        Object value = genericRecord.get(field);

        if (value == null) {
            addError(
                    errors,
                    field + NULL_ERROR
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
            GenericRecord genericRecord,
            String field,
            long min,
            long max,
            StringBuilder errors
    ) {

        Object value = genericRecord.get(field);

        if (value == null) {
            addError(
                    errors,
                    field + NULL_ERROR
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
            GenericRecord genericRecord,
            String minField,
            String maxField,
            StringBuilder errors
    ) {

        Object minObject =
                genericRecord.get(minField);

        Object maxObject =
                genericRecord.get(maxField);

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
            GenericRecord genericRecord,
            String field,
            StringBuilder errors
    ) {

        Object value = genericRecord.get(field);

        if (value == null) {
            addError(
                    errors,
                    field + NULL_ERROR
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
                            + INVALID_IPV4_ERROR
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
                                    + INVALID_IPV4_ERROR
                                    + ip
                    );
                    return;
                }
            }

        } catch (NumberFormatException e) {

            addError(
                    errors,
                    field
                            + INVALID_IPV4_ERROR
                            + ip
            );
        }
    }

    private void addError(
            StringBuilder errors,
            String error
    ) {

        if (!errors.isEmpty()) {
            errors.append(" | ");
        }

        errors.append(error);
    }
}