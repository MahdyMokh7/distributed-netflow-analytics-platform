package com.nimbo.batch.discovery;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.model.BatchStateRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FileDiscoveryService {

    private final FileSystem fileSystem;
    private final HdfsConfig hdfsConfig;
    private final BatchConfig batchConfig;
    private final BatchStateDao stateDao;

    private final Set<String> inMemoryProcessedFiles = ConcurrentHashMap.newKeySet();
    @Getter
    private boolean usingFallback = false;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("netflow-p\\d{2}-(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}Z)-.*\\.parquet");

    private static final DateTimeFormatter FILENAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'").withZone(ZoneOffset.UTC);

    public FileDiscoveryService(
            FileSystem fileSystem,
            HdfsConfig hdfsConfig,
            BatchConfig batchConfig,
            BatchStateDao stateDao) {
        this.fileSystem = fileSystem;
        this.hdfsConfig = hdfsConfig;
        this.batchConfig = batchConfig;
        this.stateDao = stateDao;
    }

    public List<Path> discoverNewFiles(UUID batchId) {
        log.info("Discovering new files for batch: {}", batchId);

        try {
            Set<String> processedFileNames = getProcessedFileNames();
            log.debug("Found {} already processed files", processedFileNames.size());

            List<Path> allFiles = listHdfsFiles();
            log.debug("Found {} total files in HDFS", allFiles.size());

            int maxFiles = batchConfig.getMaxFilesPerBatch();

            List<FileInfo> allNewFiles = new ArrayList<>();
            for (Path file : allFiles) {
                String fileName = file.getName();

                if (processedFileNames.contains(fileName)) {
                    continue;
                }

                Instant timestamp = extractTimestamp(fileName);
                if (timestamp == null) {
                    log.warn("Could not parse timestamp from filename: {}", fileName);
                    continue;
                }

                allNewFiles.add(new FileInfo(file, timestamp));
            }

            allNewFiles.sort(Comparator.comparing(f -> f.timestamp));

            if (allNewFiles.isEmpty()) {
                log.info("No new files to process (batch: {})", batchId);
                return List.of();
            }

            int limit = Math.min(allNewFiles.size(), maxFiles);
            if (allNewFiles.size() > maxFiles) {
                log.warn("Found {} new files, limiting to {} files per batch (oldest first)",
                        allNewFiles.size(), maxFiles);
            }

            List<Path> result = allNewFiles.stream()
                    .limit(limit)
                    .map(f -> f.path)
                    .collect(Collectors.toList());

            log.info("Discovered {} new files to process (batch: {})", result.size(), batchId);
            return result;

        } catch (Exception e) {
            log.error("Failed to discover files", e);
            throw new RuntimeException("File discovery failed", e);
        }
    }

    public void markFilesAsProcessed(List<Path> files, UUID batchId) {
        if (files == null || files.isEmpty()) {
            return;
        }

        log.info("Marking {} files as processed for batch: {}", files.size(), batchId);

        List<BatchStateRecord> records = new ArrayList<>();
        for (Path file : files) {
            String fileName = file.getName();
            Instant timestamp = extractTimestamp(fileName);

            if (timestamp == null) {
                log.warn("Skipping file with unparseable timestamp: {}", fileName);
                continue;
            }

            records.add(BatchStateRecord.builder()
                    .fileName(fileName)
                    .fileTimestamp(timestamp)
                    .batchId(batchId)
                    .build());
        }

        if (records.isEmpty()) {
            return;
        }

        for (BatchStateRecord record : records) {
            inMemoryProcessedFiles.add(record.getFileName());
        }

        try {
            stateDao.markFilesAsProcessed(records);
            usingFallback = false;
            log.info("Successfully marked {} files as processed in PostgreSQL", records.size());
        } catch (Exception e) {
            log.warn("Failed to mark files in PostgreSQL. State preserved in memory only. ({} files)",
                    records.size(), e);
            usingFallback = true;
        }
    }

    private Set<String> getProcessedFileNames() {
        try {
            Set<String> dbFiles = stateDao.getProcessedFileNames();
            inMemoryProcessedFiles.clear();
            inMemoryProcessedFiles.addAll(dbFiles);
            usingFallback = false;
            log.debug("Loaded {} processed files from PostgreSQL", dbFiles.size());
            return dbFiles;
        } catch (Exception e) {
            log.warn("PostgreSQL unavailable, using in-memory fallback ({} files)",
                    inMemoryProcessedFiles.size(), e);
            usingFallback = true;
            return inMemoryProcessedFiles;
        }
    }

    private List<Path> listHdfsFiles() {
        try {
            Path basePath = new Path(hdfsConfig.getFullParquetPath());
            org.apache.hadoop.fs.FileStatus[] statuses = fileSystem.listStatus(basePath);
            List<Path> files = new ArrayList<>();

            for (org.apache.hadoop.fs.FileStatus status : statuses) {
                if (!status.isDirectory()) {
                    String name = status.getPath().getName();
                    if (name.endsWith(".parquet") && !name.startsWith(".")) {
                        files.add(status.getPath());
                    }
                }
            }

            return files;
        } catch (IOException e) {
            throw new RuntimeException("Failed to list HDFS files", e);
        }
    }

    private Instant extractTimestamp(String fileName) {
        try {
            Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);
            if (!matcher.matches()) {
                log.debug("Filename does not match expected pattern: {}", fileName);
                return null;
            }
            String timestampStr = matcher.group(1);
            return Instant.from(FILENAME_FORMATTER.parse(timestampStr));
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse timestamp from: {}", fileName);
            return null;
        }
    }

    private static class FileInfo {
        final Path path;
        final Instant timestamp;

        FileInfo(Path path, Instant timestamp) {
            this.path = path;
            this.timestamp = timestamp;
        }
    }
}