package com.nimbo.batch.integration;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.discovery.FileDiscoveryService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BatchProcessorFallbackTest {

    @TempDir
    java.nio.file.Path fallbackTempDir;

    @Test
    void givenUnavailablePostgres_whenFilesAreDiscovered_thenStateFallsBackToMemory() throws Exception {
        // Given: a partitioned Parquet path and an unavailable PostgreSQL endpoint
        String fileName =
                "netflow-p00-2026-08-27T10-00-00-000Z-00000000-0000-0000-0000-000000000001.parquet";
        java.nio.file.Path hourDirectory = fallbackTempDir.resolve("2026-08-27").resolve("10");
        Files.createDirectories(hourDirectory);
        Files.createFile(hourDirectory.resolve(fileName));

        Configuration configuration = new Configuration(false);
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        configuration.setBoolean("fs.file.impl.disable.cache", true);

        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsConfig.setUri(fallbackTempDir.toUri().toString());
        hdfsConfig.setParquetPath("");

        BatchConfig batchConfig = new BatchConfig();
        batchConfig.setMaxFilesPerBatch(5);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:postgresql://127.0.0.1:1/unavailable");
        hikariConfig.setUsername("test_user");
        hikariConfig.setPassword("test_password");
        hikariConfig.setMaximumPoolSize(1);
        hikariConfig.setMinimumIdle(0);
        hikariConfig.setConnectionTimeout(250);
        hikariConfig.setValidationTimeout(250);
        hikariConfig.setInitializationFailTimeout(-1);

        try (FileSystem fileSystem = FileSystem.get(fallbackTempDir.toUri(), configuration);
                HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
            Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
            BatchStateDao stateDao = jdbi.onDemand(BatchStateDao.class);
            FileDiscoveryService discoveryService =
                    new FileDiscoveryService(fileSystem, hdfsConfig, batchConfig, stateDao);

            // When: discovery reads state through the unavailable real data source
            List<Path> discoveredFiles = discoveryService.discoverNewFiles(
                    UUID.fromString("10000000-0000-0000-0000-000000000001"));

            // Then: the file is found and state tracking reports fallback mode
            assertThat(discoveredFiles).extracting(Path::getName).containsExactly(fileName);
            assertThat(discoveryService.isUsingFallback()).isTrue();
        }
    }
}
