package com.nimbo.analyticsquery.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;

class StarRocksSemanticMetadataTest {

    @Test
    void givenColumnCommentWhenMetadataIsCreatedThenCommentIsUsedAsMeaning() {
        // Given: a physical metric column with an authoritative comment
        String comment = "Bytes retransmitted toward the destination.";

        // When: semantic metadata is derived
        var metadata =
                StarRocksSemanticMetadata.metadata("retransmitted_in_bytes", "bigint", comment)
                        .orElseThrow();

        // Then: the comment and metric role are preserved
        assertThat(metadata.meaning()).isEqualTo(comment);
        assertThat(metadata.roles()).containsExactly(SemanticColumn.Role.METRIC);
        assertThat(metadata.semanticField()).isEqualTo("RETRANSMITTED_IN_BYTES");
        assertThat(metadata.allowedAggregations()).contains(Aggregation.SUM);
    }

    @Test
    void givenKnownCodeColumnWhenMetadataIsCreatedThenItIsNotAnAdditiveMetric() {
        // Given: a numeric NetFlow code column
        String columnName = "dns_query_type";

        // When: semantic metadata is derived
        var metadata = StarRocksSemanticMetadata.metadata(columnName, "int", null).orElseThrow();

        // Then: it is exposed as a dimension with a readable fallback meaning
        assertThat(metadata.roles()).containsExactly(SemanticColumn.Role.DIMENSION);
        assertThat(metadata.meaning()).isEqualTo("Dns query type.");
        assertThat(metadata.semanticField()).isEqualTo("DNS_QUERY_TYPE");
        assertThat(metadata.allowedAggregations())
                .containsExactlyInAnyOrder(Aggregation.COUNT, Aggregation.COUNT_DISTINCT);
    }

    @Test
    void givenGenericTimestampCommentWhenMetadataIsCreatedThenCuratedUnitIsPreserved() {
        // Given: a known timestamp column with a generic physical comment
        String comment = "Kafka event timestamp";

        // When: semantic metadata is derived
        var metadata =
                StarRocksSemanticMetadata.metadata("kafka_timestamp", "bigint", comment)
                        .orElseThrow();

        // Then: the epoch-millisecond unit is preserved
        assertThat(metadata.meaning()).isEqualTo("Kafka record timestamp in epoch milliseconds.");
    }

    @Test
    void givenMeasurementColumnWhenMetadataIsCreatedThenSumIsNotAllowed() {
        // Given: a minimum TTL measurement rather than an additive counter
        String columnName = "min_ttl";

        // When: semantic metadata is derived
        var metadata = StarRocksSemanticMetadata.metadata(columnName, "int", null).orElseThrow();

        // Then: measurement aggregations exclude a meaningless sum
        assertThat(metadata.allowedAggregations())
                .contains(Aggregation.NONE, Aggregation.AVG, Aggregation.MIN, Aggregation.MAX)
                .doesNotContain(Aggregation.SUM);
    }

    @Test
    void givenAmbiguousNumericColumnWhenMetadataIsCreatedThenItIsNotExposed() {
        // Given: a numeric column with no recognized NetFlow semantics
        String columnName = "unknown_value";

        // When: semantic metadata is derived
        var metadata = StarRocksSemanticMetadata.metadata(columnName, "bigint", null);

        // Then: unsafe role guessing is avoided
        assertThat(metadata).isEmpty();
    }
}
