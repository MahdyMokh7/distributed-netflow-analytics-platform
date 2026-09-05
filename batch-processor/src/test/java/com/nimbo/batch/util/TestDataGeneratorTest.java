package com.nimbo.batch.util;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestDataGeneratorTest {

    @Test
    void givenTheSameRequestedCount_whenRowsAreGenerated_thenTheRowsAreDeterministicAndMatchTheSchema() {
        // Given: two requests for the same deterministic fixture size
        List<Row> first = TestDataGenerator.generateSampleRows(3);
        List<Row> second = TestDataGenerator.generateSampleRows(3);

        // When: the generated rows are compared by their complete values
        List<String> firstValues = first.stream().map(Row::toString).toList();
        List<String> secondValues = second.stream().map(Row::toString).toList();

        // Then: each row is repeatable and conforms to the declared field count
        assertThat(firstValues).containsExactlyElementsOf(secondValues);
        assertThat(first).allSatisfy(row -> assertThat(row.size()).isEqualTo(TestDataGenerator.getNetflowSchema().size()));
    }
}
