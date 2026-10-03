package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.model.AnalysisDataSource;

import net.sf.jsqlparser.statement.select.Select;

class SqlAstParserTest {

    private final SqlAstParser parser = new SqlAstParser();

    @Test
    void givenPostgresqlAggregateQueryWhenParsedThenSelectAstIsReturned() {
        // Given: a PostgreSQL query using the approved aggregate syntax
        GeneratedSqlCandidate candidate =
                new GeneratedSqlCandidate(
                        AnalysisDataSource.POSTGRESQL,
                        """
                        SELECT src_ip, SUM(in_bytes + out_bytes) AS total_bytes
                        FROM traffic_aggregates
                        WHERE window_start >= TIMESTAMP '2026-10-03 08:00:00'
                          AND window_start < TIMESTAMP '2026-10-03 09:00:00'
                        GROUP BY src_ip
                        ORDER BY total_bytes DESC
                        LIMIT 10
                        """);

        // When: the candidate is parsed with the PostgreSQL dialect
        ParsedSql parsedSql = parser.parse(candidate);

        // Then: one traversable SELECT statement is returned
        assertThat(parsedSql.statements()).singleElement().isInstanceOf(Select.class);
    }

    @Test
    void givenStarRocksNetflowQueryWhenParsedThenSelectAstIsReturned() {
        // Given: a StarRocks query using the restricted MySQL-compatible syntax
        GeneratedSqlCandidate candidate =
                new GeneratedSqlCandidate(
                        AnalysisDataSource.STARROCKS,
                        """
                        SELECT ipv4_src_addr,
                               COUNT(DISTINCT l4_dst_port) AS destination_port_count
                        FROM nimbo.netflow
                        WHERE kafka_timestamp >= 1791014400000
                          AND kafka_timestamp < 1791018000000
                        GROUP BY ipv4_src_addr
                        ORDER BY destination_port_count DESC
                        LIMIT 10
                        """);

        // When: the candidate is parsed with the MySQL-compatible dialect
        ParsedSql parsedSql = parser.parse(candidate);

        // Then: one traversable SELECT statement is returned
        assertThat(parsedSql.statements()).singleElement().isInstanceOf(Select.class);
    }

    @Test
    void givenMalformedSqlWhenParsedThenControlledParsingFailureIsRaised() {
        // Given: malformed generated PostgreSQL SQL
        GeneratedSqlCandidate candidate =
                new GeneratedSqlCandidate(AnalysisDataSource.POSTGRESQL, "SELECT FROM WHERE");

        // When: the malformed candidate is parsed
        var assertion = assertThatThrownBy(() -> parser.parse(candidate));

        // Then: the parser exposes a controlled parsing failure
        assertion.isInstanceOf(SqlParsingException.class).hasMessageContaining("POSTGRESQL");
    }

    @Test
    void givenMultipleStatementsWhenParsedThenAllStatementsRemainAvailableForPolicyValidation() {
        // Given: two syntactically valid statements requiring later policy rejection
        GeneratedSqlCandidate candidate =
                new GeneratedSqlCandidate(
                        AnalysisDataSource.POSTGRESQL,
                        "SELECT src_ip FROM traffic_aggregates; "
                                + "SELECT dst_ip FROM connection_aggregates");

        // When: the candidate is parsed into its syntax tree
        ParsedSql parsedSql = parser.parse(candidate);

        // Then: both statements remain visible to the Phase 3.8 validator
        assertThat(parsedSql.statements()).hasSize(2);
    }
}
