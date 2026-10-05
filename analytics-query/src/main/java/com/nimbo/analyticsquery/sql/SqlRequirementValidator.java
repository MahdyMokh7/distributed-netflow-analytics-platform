package com.nimbo.analyticsquery.sql;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;

@Component
public class SqlRequirementValidator {

    public void validate(
            ParsedSql parsedSql,
            QueryRequirements requirements,
            SqlGenerationContext context) {

        Objects.requireNonNull(parsedSql, "parsedSql must not be null");
        Objects.requireNonNull(requirements, "queryRequirements must not be null");
        Objects.requireNonNull(context, "sqlGenerationContext must not be null");

        String sql = normalize(parsedSql.sql());
        Map<String, String> expressions = context.tables().stream()
                .flatMap(table -> table.semanticExpressions().stream())
                .collect(
                        Collectors.toMap(
                                SqlGenerationContext.SemanticExpression::semanticField,
                                expression -> normalize(expression.sqlExpression()),
                                (first, ignored) -> first));

        requirements.dimensions().forEach(
                field -> require(sql, expression(field, expressions), "dimension"));

        requirements.metrics().forEach(metric -> {
            String field = directionalField(metric.field(), requirements.trafficDirection());
            String expression = expression(field, expressions);
            String expected = switch (metric.aggregation()) {
                case NONE -> expression;
                case COUNT_DISTINCT -> "countdistinct" + expression;
                default -> metric.aggregation().name().toLowerCase(Locale.ROOT) + expression;
            };
            require(sql, expected, "metric aggregation");
        });

        requirements.filters().forEach(filter -> {
            require(sql, expression(filter.field(), expressions), "filter field");
            require(sql, operator(filter.operator()), "filter operator");
            filter.values().forEach(value -> require(sql, normalize(value), "filter value"));
        });

        requirements.ipRequirements().forEach(requirement -> {
            validateIpRole(sql, requirement, expressions);
            requirement.values().forEach(value -> require(sql, normalize(value), "IP value"));
        });

        if (!requirements.groupBy().isEmpty()) {
            require(sql, "groupby", "grouping");
            requirements.groupBy().forEach(
                    field -> require(sql, expression(field, expressions), "grouping field"));
        }

        if (!requirements.sort().isEmpty()) {
            require(sql, "orderby", "sorting");
        }
        requirements.sort().forEach(sort -> {
            String field = directionalField(sort.field(), requirements.trafficDirection());
            require(sql, expression(field, expressions), "sorting field");
            require(sql, sort.direction().name().toLowerCase(Locale.ROOT), "sort direction");
        });

        if (requirements.timeRange() != null) {
            requireTime(sql, requirements.timeRange().start());
            requireTime(sql, requirements.timeRange().end());
        }

        if (requirements.limit() != null) {
            require(sql, "limit" + requirements.limit(), "limit");
        }

        if (requirements.distinct()) {
            require(sql, "selectdistinct", "distinct result");
        }
    }

    private void validateIpRole(
            String sql,
            QueryRequirements.IpRequirement requirement,
            Map<String, String> expressions) {

        if (requirement.role() == QueryRequirements.IpRole.EITHER) {
            require(sql, expression(SemanticFieldNames.SOURCE_IP, expressions), "source IP role");
            require(sql, expression(SemanticFieldNames.DESTINATION_IP, expressions), "destination IP role");
            require(sql, "or", "either-side IP matching");
            return;
        }

        String field = requirement.role() == QueryRequirements.IpRole.SOURCE
                ? SemanticFieldNames.SOURCE_IP
                : SemanticFieldNames.DESTINATION_IP;
        require(sql, expression(field, expressions), "IP role");
    }

    private String expression(String field, Map<String, String> expressions) {
        String expression = expressions.get(field);
        if (expression == null) {
            reject("No approved SQL expression exists for semantic field " + field);
        }
        return expression;
    }

    private String directionalField(
            String field,
            QueryRequirements.TrafficDirection direction) {

        if (direction == QueryRequirements.TrafficDirection.INBOUND) {
            if (SemanticFieldNames.TOTAL_BYTES.equals(field)) {
                return SemanticFieldNames.IN_BYTES;
            }
            if (SemanticFieldNames.TOTAL_PACKETS.equals(field)) {
                return SemanticFieldNames.IN_PACKETS;
            }
        }

        if (direction == QueryRequirements.TrafficDirection.OUTBOUND) {
            if (SemanticFieldNames.TOTAL_BYTES.equals(field)) {
                return SemanticFieldNames.OUT_BYTES;
            }
            if (SemanticFieldNames.TOTAL_PACKETS.equals(field)) {
                return SemanticFieldNames.OUT_PACKETS;
            }
        }

        return field;
    }

    private String operator(QueryRequirements.FilterOperator operator) {
        return switch (operator) {
            case EQ -> "=";
            case NOT_EQ -> "<>";
            case GT -> ">";
            case GTE -> ">=";
            case LT -> "<";
            case LTE -> "<=";
            case IN -> "in";
            case NOT_IN -> "notin";
            case BETWEEN -> "between";
        };
    }

    private void requireTime(String sql, Instant instant) {
        String iso = normalize(instant.toString());
        String local = normalize(instant.toString().replace("T", " ").replace("Z", ""));
        String epochMillis = Long.toString(instant.toEpochMilli());

        if (!sql.contains(iso) && !sql.contains(local) && !sql.contains(epochMillis)) {
            reject("Generated SQL does not preserve the requested time range");
        }
    }

    private void require(String sql, String expected, String name) {
        if (!sql.contains(expected)) {
            reject("Generated SQL does not preserve the requested " + name);
        }
    }

    private String normalize(String value) {
        return value.toLowerCase(Locale.ROOT)
                .replace("\"", "")
                .replace("`", "")
                .replace("'", "")
                .replace("(", "")
                .replace(")", "")
                .replaceAll("\\s+", "");
    }

    private void reject(String message) {
        throw new SqlValidationException(
                SqlPolicyViolation.SEMANTIC_REQUIREMENTS_MISMATCH,
                message);
    }
}
