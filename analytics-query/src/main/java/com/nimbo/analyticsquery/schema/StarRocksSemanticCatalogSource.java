package com.nimbo.analyticsquery.schema;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.config.StarRocksCatalogProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnProperty(
        prefix = "analytics-query.schema.starrocks",
        name = "enabled",
        havingValue = "true")
public class StarRocksSemanticCatalogSource implements SemanticCatalogSource {

    private static final int STARTUP_REFRESH_ATTEMPTS = 5;
    private static final long STARTUP_RETRY_DELAY_SECONDS = 2;

    private static final String COLUMN_QUERY =
            """
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """;

    private final JdbcTemplate jdbcTemplate;
    private final StarRocksCatalogProperties properties;

    private final AtomicReference<List<SemanticTable>> snapshot = new AtomicReference<>(List.of());

    public StarRocksSemanticCatalogSource(
            @Qualifier("starRocksCatalogJdbcTemplate") JdbcTemplate jdbcTemplate,
            StarRocksCatalogProperties properties) {

        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
    }

    @Override
    public AnalysisDataSource dataSource() {
        return AnalysisDataSource.STARROCKS;
    }

    @Override
    public Collection<SemanticTable> tables() {
        return snapshot.get();
    }

    @PostConstruct
    void initialize() {
        for (int attempt = 1; attempt <= STARTUP_REFRESH_ATTEMPTS; attempt++) {
            if (refresh() || attempt == STARTUP_REFRESH_ATTEMPTS) {
                return;
            }

            try {
                TimeUnit.SECONDS.sleep(STARTUP_RETRY_DELAY_SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                log.warn("StarRocks semantic catalog startup retry was interrupted");
                return;
            }
        }
    }

    @Scheduled(
            fixedDelayString = "${analytics-query.schema.starrocks.refresh-interval:15m}",
            initialDelayString = "${analytics-query.schema.starrocks.refresh-interval:15m}")
    public void scheduledRefresh() {
        refresh();
    }

    public synchronized boolean refresh() {
        try {
            List<PhysicalColumn> physicalColumns = loadPhysicalColumns();

            SemanticTable table = buildTable(physicalColumns);

            List<SemanticTable> candidate = List.of(table);

            if (candidate.equals(snapshot.get())) {
                log.debug("StarRocks semantic catalog for {} is unchanged", table.name());
                return true;
            }

            snapshot.set(candidate);

            log.info(
                    "Refreshed StarRocks semantic catalog for {} with {} approved columns",
                    table.name(),
                    table.columns().size());

            return true;

        } catch (DataAccessException | CatalogRefreshException exception) {

            log.warn(
                    "StarRocks semantic catalog refresh failed; retaining last known-good snapshot",
                    exception);

            return false;
        }
    }

    private List<PhysicalColumn> loadPhysicalColumns() {

        List<PhysicalColumn> columns =
                jdbcTemplate.query(
                        COLUMN_QUERY,
                        (resultSet, rowNumber) ->
                                new PhysicalColumn(
                                        resultSet.getString("COLUMN_NAME"),
                                        resultSet.getString("DATA_TYPE"),
                                        resultSet.getString("COLUMN_TYPE"),
                                        "YES".equalsIgnoreCase(resultSet.getString("IS_NULLABLE")),
                                        resultSet.getString("COLUMN_COMMENT")),
                        properties.database(),
                        properties.table());

        if (columns.isEmpty()) {
            throw new CatalogRefreshException("StarRocks table was not found");
        }

        return columns;
    }

    private SemanticTable buildTable(List<PhysicalColumn> physicalColumns) {

        List<SemanticColumn> approved =
                physicalColumns.stream()
                        .map(this::toSemanticColumn)
                        .flatMap(Optional::stream)
                        .toList();

        boolean hasTimestamp =
                approved.stream().anyMatch(column -> column.name().equals("process_time"));

        if (!hasTimestamp) {
            throw new CatalogRefreshException("Approved StarRocks timestamp column is unavailable");
        }

        return new SemanticTable(
                properties.database() + "." + properties.table(),
                AnalysisDataSource.STARROCKS,
                "Raw/recent NetFlow records stored in StarRocks.",
                "one ingested NetFlow record",
                "process_time",
                null,
                approved,
                List.of(
                        "Contains raw NetFlow dimensions that are not retained by PostgreSQL aggregate cubes."),
                List.of(
                        "Source/destination IP analysis.",
                        "Source/destination port analysis.",
                        "Protocol analysis.",
                        "Distinct-count analysis over approved raw dimensions.",
                        "Raw flow byte, packet and duration analysis."),
                List.of(
                        "Security verdicts such as declaring an IP malicious or compromised.",
                        "Cross-datasource joins.",
                        "Analysis using StarRocks columns whose semantic role cannot be inferred safely."));
    }

    private Optional<SemanticColumn> toSemanticColumn(PhysicalColumn column) {

        Optional<StarRocksSemanticMetadata.ColumnMetadata> metadata =
                StarRocksSemanticMetadata.metadata(
                        column.name(), column.dataType(), column.comment());

        if (metadata.isEmpty()) {
            log.warn(
                    "Ignoring StarRocks column {} because its semantic role cannot be inferred safely",
                    column.name());
            return Optional.empty();
        }

        StarRocksSemanticMetadata.ColumnMetadata resolved = metadata.get();

        return Optional.of(
                new SemanticColumn(
                        column.name(),
                        column.columnType(),
                        resolved.meaning(),
                        resolved.roles(),
                        column.nullable(),
                        resolved.semanticField(),
                        resolved.allowedAggregations()));
    }

    private record PhysicalColumn(
            String name, String dataType, String columnType, boolean nullable, String comment) {

        private PhysicalColumn {
            name = name.toLowerCase(Locale.ROOT);
            dataType = dataType.toLowerCase(Locale.ROOT);
        }
    }

    private static final class CatalogRefreshException extends RuntimeException {

        private CatalogRefreshException(String message) {
            super(message);
        }
    }
}
