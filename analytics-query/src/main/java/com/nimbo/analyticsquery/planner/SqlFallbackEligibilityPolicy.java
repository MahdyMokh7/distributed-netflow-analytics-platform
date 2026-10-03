package com.nimbo.analyticsquery.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import com.nimbo.analyticsquery.schema.SemanticSchemaCatalog;
import com.nimbo.analyticsquery.schema.SemanticTable;

@Component
public class SqlFallbackEligibilityPolicy {

    private final SemanticSchemaCatalog catalog;

    public SqlFallbackEligibilityPolicy(SemanticSchemaCatalog catalog) {
        this.catalog = catalog;
    }

    public SqlFallbackEligibility evaluate(QueryRequirements requirements) {

        Objects.requireNonNull(requirements, "queryRequirements must not be null");

        Collection<SemanticTable> tables = catalog.tables();

        if (tables.stream().anyMatch(table -> coversRequiredFields(List.of(table), requirements))) {
            return SqlFallbackEligibility.allow();
        }

        if (canSingleDataSourceSatisfy(tables, requirements)) {

            return SqlFallbackEligibility.deny(
                    AnalysisStatus.UNSUPPORTED_ANALYSIS,
                    "Request requires combining multiple semantic tables, "
                            + "which is outside V1 SQL fallback scope");
        }

        if (coversRequiredFields(tables, requirements)) {
            return SqlFallbackEligibility.deny(
                    AnalysisStatus.UNSUPPORTED_ANALYSIS,
                    "Request requires combining multiple physical datasources");
        }

        return SqlFallbackEligibility.deny(
                AnalysisStatus.UNSUPPORTED_ANALYSIS,
                "Required analytics information is not available "
                        + "from the approved semantic schema");
    }

    private boolean canSingleDataSourceSatisfy(
            Collection<SemanticTable> tables, QueryRequirements requirements) {

        Map<AnalysisDataSource, Collection<SemanticTable>> tablesByDataSource = new HashMap<>();

        for (SemanticTable table : tables) {
            tablesByDataSource
                    .computeIfAbsent(table.dataSource(), ignored -> new java.util.ArrayList<>())
                    .add(table);
        }

        return tablesByDataSource.values().stream()
                .anyMatch(dataSourceTables -> coversRequiredFields(dataSourceTables, requirements));
    }

    private boolean coversRequiredFields(
            Collection<SemanticTable> tables, QueryRequirements requirements) {

        if (tables.isEmpty()) {
            return false;
        }

        Set<String> availableFields = collectAvailableFields(tables);

        if (!availableFields.containsAll(requiredSemanticFields(requirements))) {

            return false;
        }

        return supportsIpRequirements(availableFields, requirements.ipRequirements());
    }

    private Set<String> collectAvailableFields(Collection<SemanticTable> tables) {

        Set<String> fields = new HashSet<>();

        for (SemanticTable table : tables) {
            fields.addAll(table.semanticFields());
        }

        return fields;
    }

    private Set<String> requiredSemanticFields(QueryRequirements requirements) {

        Set<String> fields = new HashSet<>(requirements.dimensions());

        requirements.metrics().forEach(metric -> fields.add(metric.field()));

        requirements.filters().forEach(filter -> fields.add(filter.field()));

        fields.addAll(requirements.groupBy());

        requirements.sort().forEach(sort -> fields.add(sort.field()));

        return fields;
    }

    private boolean supportsIpRequirements(
            Set<String> availableFields, List<QueryRequirements.IpRequirement> ipRequirements) {

        for (QueryRequirements.IpRequirement requirement : ipRequirements) {

            boolean supported =
                    switch (requirement.role()) {
                        case SOURCE -> availableFields.contains(SemanticFieldNames.SOURCE_IP);

                        case DESTINATION -> availableFields.contains(
                                SemanticFieldNames.DESTINATION_IP);

                        case EITHER -> availableFields.contains(SemanticFieldNames.IP_ADDRESS)
                                || availableFields.contains(SemanticFieldNames.SOURCE_IP)
                                        && availableFields.contains(
                                                SemanticFieldNames.DESTINATION_IP);
                    };

            if (!supported) {
                return false;
            }
        }

        return true;
    }
}
