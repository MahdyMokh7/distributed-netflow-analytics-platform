package com.nimbo.batch.discovery;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.metrics.BatchProcessorMetrics;
import com.nimbo.batch.metrics.BatchProcessorMetrics.Stage;
import com.nimbo.batch.model.BatchStateRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
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

@Slf4j
@Component
public class FileDiscoveryService {

    private final FileSystem fileSystem;
    private final HdfsConfig hdfsConfig;
    private final BatchConfig batchConfig;
    private final BatchStateDao stateDao;
    private final BatchProcessorMetrics metrics;

    private final Set<String> inMemoryProcessedFiles = ConcurrentHashMap.newKeySet();
    @Getter
    private volatile boolean usingFallback = false;

    @Getter
    private volatile UUID failedBatchId = null;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("netflow-p\\d{2,}-(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}Z)-.*\\.parquet");

    private static final DateTimeFormatter FILENAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_DIRECTORY_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(java.time.format.ResolverStyle.STRICT);
    private static final Pattern HOUR_DIRECTORY_PATTERN = Pattern.compile("(?:[01]\\d|2[0-3])");

    public FileDiscoveryService(
            FileSystem fileSystem,
            HdfsConfig hdfsConfig,
            BatchConfig batchConfig,
            BatchStateDao stateDao,
            BatchProcessorMetrics metrics) {
        this.fileSystem = fileSystem;
        this.hdfsConfig = hdfsConfig;
        this.batchConfig = batchConfig;
        this.stateDao = stateDao;
        this.metrics = metrics;
    }

    public List<Path> discoverNewFiles(UUID batchId) {
        log.info("Discovering new files for batch: {}", batchId);

        try {
            Set<String> processedFileNames = getProcessedFileNames();
            log.debug("Found {} already processed files", processedFileNames.size());

            int maxFiles = batchConfig.getMaxFilesPerBatch();
            List<Path> newFiles = listOldestNewFiles(processedFileNames, maxFiles);

            if (newFiles.isEmpty()) {
                log.info("No new files to process (batch: {})", batchId);
                return List.of();
            }

            log.info("Discovered {} new files to process (batch: {})", newFiles.size(), batchId);
            return newFiles;

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
            setUsingFallback(false);
            Instant newestTimestamp = records.stream()
                    .map(BatchStateRecord::getFileTimestamp)
                    .max(Comparator.naturalOrder())
                    .orElse(null);
            metrics.filesProcessed(records.size(), newestTimestamp);
            log.info("Successfully marked {} files as processed in PostgreSQL", records.size());
        } catch (Exception e) {
            log.warn("Failed to mark files in PostgreSQL. State preserved in memory only. ({} files)",
                    records.size(), e);
            setUsingFallback(true);
            metrics.stageFailed(Stage.STATE_COMMIT);
        }
    }

    private Set<String> getProcessedFileNames() {
        try {
            Set<String> dbFiles = stateDao.getProcessedFileNames();
            inMemoryProcessedFiles.clear();
            inMemoryProcessedFiles.addAll(dbFiles);
            setUsingFallback(false);
            log.debug("Loaded {} processed files from PostgreSQL", dbFiles.size());
            return dbFiles;
        } catch (Exception e) {
            log.warn("PostgreSQL unavailable, using in-memory fallback ({} files)",
                    inMemoryProcessedFiles.size(), e);
            setUsingFallback(true);
            return inMemoryProcessedFiles;
        }
    }

    private List<Path> listOldestNewFiles(Set<String> processedFileNames, int maxFiles) {
        if (maxFiles <= 0) {
            return List.of();
        }

        try {
            Path basePath = new Path(hdfsConfig.getFullParquetPath());
            List<FileStatus> dateDirectories = validDateDirectories(fileSystem.listStatus(basePath));
            List<Path> result = new ArrayList<>(maxFiles);

            for (FileStatus dateDirectory : dateDirectories) {
                LocalDate date = parseDateDirectory(dateDirectory.getPath().getName());
                List<FileStatus> hourDirectories = validHourDirectories(fileSystem.listStatus(dateDirectory.getPath()));

                for (FileStatus hourDirectory : hourDirectories) {
                    int hour = Integer.parseInt(hourDirectory.getPath().getName());
                    List<FileInfo> files = validFiles(fileSystem.listStatus(hourDirectory.getPath()), date, hour);

                    for (FileInfo file : files) {
                        if (!processedFileNames.contains(file.path.getName())) {
                            result.add(file.path);
                            if (result.size() == maxFiles) {
                                return result;
                            }
                        }
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to list HDFS files", e);
        }
    }

    private List<FileStatus> validDateDirectories(FileStatus[] statuses) {
        List<FileStatus> directories = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (status.isDirectory() && parseDateDirectory(status.getPath().getName()) != null) {
                directories.add(status);
            }
        }
        directories.sort(Comparator.comparing(status -> parseDateDirectory(status.getPath().getName())));
        return directories;
    }

    private List<FileStatus> validHourDirectories(FileStatus[] statuses) {
        List<FileStatus> directories = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (status.isDirectory() && HOUR_DIRECTORY_PATTERN.matcher(status.getPath().getName()).matches()) {
                directories.add(status);
            }
        }
        directories.sort(Comparator.comparingInt(status -> Integer.parseInt(status.getPath().getName())));
        return directories;
    }

    private List<FileInfo> validFiles(FileStatus[] statuses, LocalDate directoryDate, int directoryHour) {
        List<FileInfo> files = new ArrayList<>();
        for (FileStatus status : statuses) {
            String fileName = status.getPath().getName();
            if (status.isDirectory() || fileName.startsWith(".") || !fileName.endsWith(".parquet")) {
                continue;
            }

            Instant timestamp = extractTimestamp(fileName);
            if (timestamp == null) {
                log.warn("Could not parse timestamp from filename: {}", fileName);
                continue;
            }

            var timestampUtc = timestamp.atZone(ZoneOffset.UTC);
            if (!timestampUtc.toLocalDate().equals(directoryDate) || timestampUtc.getHour() != directoryHour) {
                log.warn("Ignoring Parquet file outside its timestamp partition: {}", status.getPath());
                continue;
            }
            files.add(new FileInfo(status.getPath(), timestamp));
        }
        files.sort(Comparator.comparing(file -> file.timestamp));
        return files;
    }

    private LocalDate parseDateDirectory(String name) {
        try {
            return LocalDate.parse(name, DATE_DIRECTORY_FORMATTER);
        } catch (DateTimeParseException e) {
            return null;
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

    public void storeFailedBatch(UUID batchId) {
        this.failedBatchId = batchId;
        metrics.failedBatchPending(true);
        log.info("Stored failed batch ID: {} for retry", batchId);
    }

    public void clearFailedBatch() {
        if (failedBatchId != null) {
            log.info("Clearing failed batch: {}", failedBatchId);
            failedBatchId = null;
            metrics.failedBatchPending(false);
        }
    }

    private void setUsingFallback(boolean active) {
        usingFallback = active;
        metrics.stateFallbackActive(active);
    }
}
