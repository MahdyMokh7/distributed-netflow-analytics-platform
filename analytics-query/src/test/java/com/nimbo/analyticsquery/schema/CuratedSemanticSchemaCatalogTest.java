package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CuratedSemanticSchemaCatalogTest {

    private final CuratedSemanticSchemaCatalog catalog =
            new CuratedSemanticSchemaCatalog(java.util.List.of());

    @Test
    void givenApprovedDimensionWhenLookedUpThenCapabilitiesAreReturned() {
        // Given: an approved SOURCE_IP semantic field
        String fieldName = "SOURCE_IP";

        // When: the field is retrieved from the curated catalog
        SemanticField field = catalog.find(fieldName).orElseThrow();

        // Then: its dimension and aggregation capabilities match the contract
        assertThat(field.usages())
                .contains(
                        SemanticField.Usage.DIMENSION,
                        SemanticField.Usage.METRIC);
        assertThat(field.allowedAggregations())
                .containsExactlyInAnyOrder(
                        QueryRequirements.Aggregation.COUNT,
                        QueryRequirements.Aggregation.COUNT_DISTINCT);
        assertThat(field.filterable()).isTrue();
        assertThat(field.groupable()).isTrue();
        assertThat(field.sortable()).isTrue();
    }

    @Test
    void givenApprovedNumericMetricWhenLookedUpThenMetricCapabilitiesAreReturned() {
        // Given: an approved TOTAL_BYTES metric
        String fieldName = "TOTAL_BYTES";

        // When: the field is retrieved from the curated catalog
        SemanticField field = catalog.find(fieldName).orElseThrow();

        // Then: its supported numeric aggregations are exposed
        assertThat(field.usages())
                .containsExactly(SemanticField.Usage.METRIC);
        assertThat(field.allowedAggregations())
                .containsExactlyInAnyOrder(
                        QueryRequirements.Aggregation.NONE,
                        QueryRequirements.Aggregation.SUM,
                        QueryRequirements.Aggregation.AVG,
                        QueryRequirements.Aggregation.MIN,
                        QueryRequirements.Aggregation.MAX);
        assertThat(field.groupable()).isFalse();
    }

    @Test
    void givenPhysicalButUnapprovedFieldWhenLookedUpThenFieldIsUnavailable() {
        // Given: a StarRocks physical field not approved semantically
        String fieldName = "process_time";

        // When: the catalog is queried for that field
        var result = catalog.find(fieldName);

        // Then: physical existence does not make the field semantically available
        assertThat(result).isEmpty();
    }
}
