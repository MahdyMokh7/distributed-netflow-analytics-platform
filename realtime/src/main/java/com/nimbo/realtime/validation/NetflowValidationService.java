package com.nimbo.realtime.validation;

import com.nimbo.realtime.mapper.SchemaMapping;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class NetflowValidationService {

    private static final String FORMAT = "validationFormat";
    private static final String MIN = "validationMin";
    private static final String MAX = "validationMax";
    private static final String FINITE = "validationFinite";
    private static final String LTE_FIELD = "validationLteField";

    private final SchemaMapping schemaMapping;

    public ValidationResult validate(GenericRecord record) {

        if (record == null) {
            return ValidationResult.failure("Record is null");
        }

        StringBuilder errors = new StringBuilder();

        for (Schema.Field field : schemaMapping.fields()) {

            Object value = record.get(field.name());

            if (value == null) {
                addError(
                        errors,
                        field.name() + " is null"
                );
                continue;
            }

            validateFormat(
                    field,
                    value,
                    errors
            );

            validateNumber(
                    field,
                    value,
                    errors
            );

            validateRelation(
                    field,
                    value,
                    record,
                    errors
            );
        }

        if (errors.isEmpty()) {
            return ValidationResult.success();
        }

        return ValidationResult.failure(
                errors.toString()
        );
    }

    private void validateFormat(
            Schema.Field field,
            Object value,
            StringBuilder errors
    ) {

        String format =
                field.getProp(FORMAT);

        if (format == null) {
            return;
        }

        if ("ipv4".equalsIgnoreCase(format)
                && !isValidIpv4(value.toString())) {

            addError(
                    errors,
                    field.name()
                            + " invalid IPv4: "
                            + value
            );
        }
    }

    private void validateNumber(
            Schema.Field field,
            Object value,
            StringBuilder errors
    ) {

        if (!(value instanceof Number number)) {
            return;
        }

        double numericValue =
                number.doubleValue();

        String finite =
                field.getProp(FINITE);

        if (Boolean.parseBoolean(finite)
                && !Double.isFinite(numericValue)) {

            addError(
                    errors,
                    field.name()
                            + " must be finite: "
                            + numericValue
            );

            return;
        }

        String min =
                field.getProp(MIN);

        if (min != null) {

            double minValue =
                    Double.parseDouble(min);

            if (numericValue < minValue) {

                addError(
                        errors,
                        field.name()
                                + " below minimum "
                                + min
                                + ": "
                                + value
                );
            }
        }

        String max =
                field.getProp(MAX);

        if (max != null) {

            double maxValue =
                    Double.parseDouble(max);

            if (numericValue > maxValue) {

                addError(
                        errors,
                        field.name()
                                + " above maximum "
                                + max
                                + ": "
                                + value
                );
            }
        }
    }

    private void validateRelation(
            Schema.Field field,
            Object value,
            GenericRecord record,
            StringBuilder errors
    ) {

        String otherFieldName =
                field.getProp(LTE_FIELD);

        if (otherFieldName == null) {
            return;
        }

        Object otherValue =
                record.get(otherFieldName);

        if (!(value instanceof Number left)
                || !(otherValue instanceof Number right)) {

            return;
        }

        if (Double.compare(
                left.doubleValue(),
                right.doubleValue()
        ) > 0) {

            addError(
                    errors,
                    field.name()
                            + " > "
                            + otherFieldName
                            + " ("
                            + value
                            + " > "
                            + otherValue
                            + ")"
            );
        }
    }

    private boolean isValidIpv4(String ip) {

        String[] parts =
                ip.split("\\.", -1);

        if (parts.length != 4) {
            return false;
        }

        for (String part : parts) {

            if (part.isEmpty()
                    || part.length() > 3) {

                return false;
            }

            for (int i = 0; i < part.length(); i++) {

                if (!Character.isDigit(
                        part.charAt(i)
                )) {

                    return false;
                }
            }

            int value;

            try {
                value =
                        Integer.parseInt(part);
            } catch (NumberFormatException e) {
                return false;
            }

            if (value > 255) {
                return false;
            }
        }

        return true;
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