package com.nimbo.analyticsquery.intent;

import com.nimbo.analyticsquery.schema.CuratedSemanticSchemaCatalog;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class IntentPromptFactoryTest {

    private static final Instant NOW =
            Instant.parse("2026-10-01T10:00:00Z");

    private final IntentPromptFactory promptFactory =
            new IntentPromptFactory(
                    Clock.fixed(NOW, ZoneOffset.UTC),
                    new CuratedSemanticSchemaCatalog());

    @Test
    void givenFixedUtcClockWhenPromptIsCreatedThenTimeContextIsDeterministic() {
        // Given: a fixed UTC interpretation time
        String expectedStart = "2026-10-01T09:00:00Z";

        // When: the intent system prompt is created
        String prompt = promptFactory.createSystemPrompt();

        // Then: current time and example range use deterministic UTC values
        assertThat(prompt)
                .contains("2026-10-01T10:00:00Z")
                .contains(expectedStart);
    }

    @Test
    void givenCuratedCatalogWhenPromptIsCreatedThenOnlyApprovedSemanticsAreExposed() {
        // Given: the current curated semantic catalog
        String physicalStarRocksColumn = "kafka_timestamp";

        // When: the intent system prompt is created
        String prompt = promptFactory.createSystemPrompt();

        // Then: approved semantics are present and physical fields remain hidden
        assertThat(prompt)
                .contains("SOURCE_IP")
                .contains("DESTINATION_IP")
                .contains("TOTAL_BYTES")
                .contains("CONNECTION_COUNT")
                .doesNotContain(physicalStarRocksColumn)
                .doesNotContain("ipv4_src_addr")
                .doesNotContain("retransmitted_out_pkts");
    }

    @Test
    void givenIntentContractWhenPromptIsCreatedThenExecutionInstructionsAreForbidden() {
        // Given: the production intent prompt contract
        String forbiddenRule = "Never return or invent:";

        // When: the system prompt is created
        String prompt = promptFactory.createSystemPrompt();

        // Then: execution and physical-routing outputs are explicitly prohibited
        assertThat(prompt)
                .contains(forbiddenRule)
                .contains("- SQL")
                .contains("- database names")
                .contains("- physical column names")
                .contains("- datasource selection")
                .contains("- REST endpoints")
                .contains("- tool names");
    }
}