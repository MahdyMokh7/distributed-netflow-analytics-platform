package com.nimbo.analyticsquery.schema;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

@Component
public class CuratedSemanticSchemaCatalog implements SemanticSchemaCatalog {

    private static final Set<Aggregation> ADDITIVE_AGGREGATIONS =
            Set.of(
                    Aggregation.NONE,
                    Aggregation.SUM,
                    Aggregation.AVG,
                    Aggregation.MIN,
                    Aggregation.MAX);

    private static final Set<Aggregation> DIMENSION_AGGREGATIONS =
            Set.of(Aggregation.COUNT, Aggregation.COUNT_DISTINCT);

    private final Map<String, SemanticField> curatedFields;
    private final List<SemanticCatalogSource> sources;

    public CuratedSemanticSchemaCatalog(List<SemanticCatalogSource> sources) {

        this.sources = List.copyOf(sources);

        Map<String, SemanticField> configuredFields = new LinkedHashMap<>();

        add(
                configuredFields,
                dimension(
                        SemanticFieldNames.SOURCE_IP,
                        "Source IP address observed in a NetFlow record."));

        add(
                configuredFields,
                dimension(
                        SemanticFieldNames.DESTINATION_IP,
                        "Destination IP address observed in a NetFlow record."));

        add(
                configuredFields,
                dimension(
                        SemanticFieldNames.IP_ADDRESS,
                        "IP address viewed from either the source or destination perspective."));

        add(
                configuredFields,
                dimension(
                        SemanticFieldNames.L7_PROTOCOL,
                        "Layer-7 protocol identifier associated with the traffic."));

        add(
                configuredFields,
                metric(
                        SemanticFieldNames.IN_BYTES,
                        "Bytes received in the selected NetFlow perspective."));

        add(
                configuredFields,
                metric(
                        SemanticFieldNames.OUT_BYTES,
                        "Bytes sent in the selected NetFlow perspective."));

        add(
                configuredFields,
                metric(SemanticFieldNames.TOTAL_BYTES, "Combined inbound and outbound bytes."));

        add(
                configuredFields,
                metric(
                        SemanticFieldNames.IN_PACKETS,
                        "Packets received in the selected NetFlow perspective."));

        add(
                configuredFields,
                metric(
                        SemanticFieldNames.OUT_PACKETS,
                        "Packets sent in the selected NetFlow perspective."));

        add(
                configuredFields,
                metric(SemanticFieldNames.TOTAL_PACKETS, "Combined inbound and outbound packets."));

        add(
                configuredFields,
                metric(
                        SemanticFieldNames.RECORD_COUNT,
                        "Number of NetFlow records represented by an aggregate."));

        add(
                configuredFields,
                metric(
                        SemanticFieldNames.CONNECTION_COUNT,
                        "Number of NetFlow connections represented by an aggregate."));

        this.curatedFields = Map.copyOf(configuredFields);
    }

    @Override
    public Collection<SemanticField> fields() {
        return currentFields().values();
    }

    @Override
    public Optional<SemanticField> find(String name) {
        return Optional.ofNullable(currentFields().get(name));
    }

    @Override
    public Collection<SemanticTable> tables() {
        return sources.stream().flatMap(source -> source.tables().stream()).toList();
    }

    @Override
    public Collection<SemanticTable> tables(AnalysisDataSource dataSource) {

        return sources.stream()
                .filter(source -> source.dataSource() == dataSource)
                .flatMap(source -> source.tables().stream())
                .toList();
    }

    @Override
    public Optional<SemanticTable> findTable(AnalysisDataSource dataSource, String tableName) {

        if (dataSource == null || tableName == null || tableName.isBlank()) {

            return Optional.empty();
        }

        return tables(dataSource).stream()
                .filter(table -> table.name().equals(tableName))
                .findFirst();
    }

    private static SemanticField dimension(String name, String description) {

        return new SemanticField(
                name,
                description,
                Set.of(SemanticField.Usage.DIMENSION, SemanticField.Usage.METRIC),
                DIMENSION_AGGREGATIONS,
                true,
                true,
                true);
    }

    private static SemanticField metric(String name, String description) {

        return new SemanticField(
                name,
                description,
                Set.of(SemanticField.Usage.METRIC),
                ADDITIVE_AGGREGATIONS,
                true,
                false,
                true);
    }

    private static void add(Map<String, SemanticField> configuredFields, SemanticField field) {

        SemanticField previous = configuredFields.putIfAbsent(field.name(), field);

        if (previous != null) {
            throw new IllegalStateException("Duplicate semantic field: " + field.name());
        }
    }

    private Map<String, SemanticField> currentFields() {
        Map<String, SemanticField> current = new LinkedHashMap<>(curatedFields);

        tables().stream()
                .flatMap(table -> table.columns().stream())
                .filter(column -> column.semanticField() != null)
                .forEach(column -> current.putIfAbsent(column.semanticField(), fromColumn(column)));

        return Map.copyOf(current);
    }

    private static SemanticField fromColumn(SemanticColumn column) {

        boolean dimension = column.isDimension();
        Set<SemanticField.Usage> usages =
                dimension
                        ? Set.of(SemanticField.Usage.DIMENSION, SemanticField.Usage.METRIC)
                        : Set.of(SemanticField.Usage.METRIC);

        return new SemanticField(
                column.semanticField(),
                column.meaning(),
                usages,
                column.allowedAggregations(),
                true,
                dimension,
                true);
    }
}
