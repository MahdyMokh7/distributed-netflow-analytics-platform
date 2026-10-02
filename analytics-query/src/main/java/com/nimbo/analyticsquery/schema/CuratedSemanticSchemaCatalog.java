package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Component
public class CuratedSemanticSchemaCatalog implements SemanticSchemaCatalog {

    private static final Set<Aggregation> ADDITIVE_AGGREGATIONS = Set.of(
            Aggregation.NONE,
            Aggregation.SUM,
            Aggregation.AVG,
            Aggregation.MIN,
            Aggregation.MAX);

    private static final Set<Aggregation> DIMENSION_AGGREGATIONS = Set.of(
            Aggregation.COUNT,
            Aggregation.COUNT_DISTINCT);

    private final Map<String, SemanticField> fields;

    public CuratedSemanticSchemaCatalog() {
        Map<String, SemanticField> configuredFields = new LinkedHashMap<>();

        add(configuredFields, dimension(
                "SOURCE_IP",
                "Source IP address observed in a NetFlow record."));
        add(configuredFields, dimension(
                "DESTINATION_IP",
                "Destination IP address observed in a NetFlow record."));
        add(configuredFields, dimension(
                "IP_ADDRESS",
                "IP address viewed from either the source or destination perspective."));
        add(configuredFields, dimension(
                "L7_PROTOCOL",
                "Layer-7 protocol identifier associated with the traffic."));

        add(configuredFields, metric(
                "IN_BYTES",
                "Bytes received in the selected NetFlow perspective."));
        add(configuredFields, metric(
                "OUT_BYTES",
                "Bytes sent in the selected NetFlow perspective."));
        add(configuredFields, metric(
                "TOTAL_BYTES",
                "Combined inbound and outbound bytes."));
        add(configuredFields, metric(
                "IN_PACKETS",
                "Packets received in the selected NetFlow perspective."));
        add(configuredFields, metric(
                "OUT_PACKETS",
                "Packets sent in the selected NetFlow perspective."));
        add(configuredFields, metric(
                "TOTAL_PACKETS",
                "Combined inbound and outbound packets."));
        add(configuredFields, metric(
                "RECORD_COUNT",
                "Number of NetFlow records represented by an aggregate."));
        add(configuredFields, metric(
                "CONNECTION_COUNT",
                "Number of NetFlow connections represented by an aggregate."));

        fields = Map.copyOf(configuredFields);
    }

    @Override
    public Collection<SemanticField> fields() {
        return fields.values();
    }

    @Override
    public Optional<SemanticField> find(String name) {
        return Optional.ofNullable(fields.get(name));
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

    private static void add(
            Map<String, SemanticField> configuredFields,
            SemanticField field) {

        configuredFields.put(field.name(), field);
    }
}
