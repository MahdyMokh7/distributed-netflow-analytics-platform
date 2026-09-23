package com.nimbo.batch.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public final class NetFlowParquetTestData {

    private static final String[] IP_ADDRESSES = {
            "192.168.1.100",
            "192.168.1.101",
            "192.168.1.102",
            "10.0.0.1",
            "10.0.0.2",
            "10.0.0.3",
            "172.16.0.1",
            "172.16.0.2",
            "172.16.0.3"
    };

    private static final int[] PROTOCOLS = {6, 17, 1, 2};
    private static final int[] PORTS = {
            80, 443, 53, 22, 25, 123, 3306, 5432, 8080, 8443
    };

    private static final DateTimeFormatter FILENAME_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'")
                    .withZone(ZoneOffset.UTC);

    private NetFlowParquetTestData() {
    }

    public static StructType getNetflowSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("IPV4_SRC_ADDR", DataTypes.StringType, false),
                DataTypes.createStructField("IPV4_DST_ADDR", DataTypes.StringType, false),
                DataTypes.createStructField("L4_SRC_PORT", DataTypes.IntegerType, false),
                DataTypes.createStructField("L4_DST_PORT", DataTypes.IntegerType, false),
                DataTypes.createStructField("PROTOCOL", DataTypes.IntegerType, false),
                DataTypes.createStructField("L7_PROTO", DataTypes.DoubleType, false),
                DataTypes.createStructField("IN_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("OUT_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("IN_PKTS", DataTypes.LongType, false),
                DataTypes.createStructField("OUT_PKTS", DataTypes.LongType, false),
                DataTypes.createStructField("FLOW_DURATION_MILLISECONDS", DataTypes.LongType, false),
                DataTypes.createStructField("TCP_FLAGS", DataTypes.IntegerType, false),
                DataTypes.createStructField("CLIENT_TCP_FLAGS", DataTypes.IntegerType, false),
                DataTypes.createStructField("SERVER_TCP_FLAGS", DataTypes.IntegerType, false),
                DataTypes.createStructField("DURATION_IN", DataTypes.LongType, false),
                DataTypes.createStructField("DURATION_OUT", DataTypes.LongType, false),
                DataTypes.createStructField("MIN_TTL", DataTypes.IntegerType, false),
                DataTypes.createStructField("MAX_TTL", DataTypes.IntegerType, false),
                DataTypes.createStructField("LONGEST_FLOW_PKT", DataTypes.IntegerType, false),
                DataTypes.createStructField("SHORTEST_FLOW_PKT", DataTypes.IntegerType, false),
                DataTypes.createStructField("MIN_IP_PKT_LEN", DataTypes.IntegerType, false),
                DataTypes.createStructField("MAX_IP_PKT_LEN", DataTypes.IntegerType, false),
                DataTypes.createStructField("SRC_TO_DST_SECOND_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("DST_TO_SRC_SECOND_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("RETRANSMITTED_IN_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("RETRANSMITTED_IN_PKTS", DataTypes.LongType, false),
                DataTypes.createStructField("RETRANSMITTED_OUT_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("RETRANSMITTED_OUT_PKTS", DataTypes.LongType, false),
                DataTypes.createStructField("SRC_TO_DST_AVG_THROUGHPUT", DataTypes.LongType, false),
                DataTypes.createStructField("DST_TO_SRC_AVG_THROUGHPUT", DataTypes.LongType, false),
                DataTypes.createStructField("NUM_PKTS_UP_TO_128_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("NUM_PKTS_128_TO_256_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("NUM_PKTS_256_TO_512_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("NUM_PKTS_512_TO_1024_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("NUM_PKTS_1024_TO_1514_BYTES", DataTypes.LongType, false),
                DataTypes.createStructField("TCP_WIN_MAX_IN", DataTypes.IntegerType, false),
                DataTypes.createStructField("TCP_WIN_MAX_OUT", DataTypes.IntegerType, false),
                DataTypes.createStructField("ICMP_TYPE", DataTypes.IntegerType, false),
                DataTypes.createStructField("ICMP_IPV4_TYPE", DataTypes.IntegerType, false),
                DataTypes.createStructField("DNS_QUERY_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("DNS_QUERY_TYPE", DataTypes.IntegerType, false),
                DataTypes.createStructField("DNS_TTL_ANSWER", DataTypes.LongType, false),
                DataTypes.createStructField("FTP_COMMAND_RET_CODE", DataTypes.IntegerType, false),
                DataTypes.createStructField("PROCESS_TIME", DataTypes.LongType, false)
        });
    }

    public static List<Row> generateRowsForFile(
            int fileIndex,
            int recordsPerFile,
            Instant baseTimestamp,
            int partition) {

        List<Row> rows = new ArrayList<>(recordsPerFile);

        for (int recordIdx = 0; recordIdx < recordsPerFile; recordIdx++) {
            Instant recordTime = baseTimestamp
                    .plusSeconds(fileIndex * 30L)
                    .plusMillis(recordIdx * 10L);

            long processTimeMicros = recordTime.getEpochSecond() * 1_000_000L
                    + recordTime.getNano() / 1000;

            int ipIdx = (fileIndex + recordIdx) % IP_ADDRESSES.length;
            String srcIp = IP_ADDRESSES[ipIdx];
            String dstIp = IP_ADDRESSES[(ipIdx + 1) % IP_ADDRESSES.length];

            int protoIdx = (fileIndex + recordIdx) % PROTOCOLS.length;
            int protocol = PROTOCOLS[protoIdx];

            int portIdx = (fileIndex + recordIdx) % PORTS.length;
            int srcPort = PORTS[portIdx];
            int dstPort = PORTS[(portIdx + 1) % PORTS.length];

            double l7Proto = switch (protocol) {
                case 6 -> 6.0;
                case 17 -> 17.0;
                case 1 -> 1.0;
                default -> 2.0;
            };

            long baseBytes = 1000L + (fileIndex * 100L) + (recordIdx * 10L);
            long inBytes = baseBytes;
            long outBytes = baseBytes / 2;
            long inPackets = 10L + fileIndex + recordIdx;
            long outPackets = 5L + fileIndex + recordIdx;
            long duration = 100L + (fileIndex % 1000) + (recordIdx % 100);

            Row row = RowFactory.create(
                    srcIp, dstIp,
                    srcPort, dstPort, protocol,
                    l7Proto,
                    inBytes, outBytes, inPackets, outPackets,
                    duration,
                    16, 8, 8,
                    duration / 2, duration / 2,
                    32, 64,
                    1500, 64, 64, 1500,
                    inBytes / Math.max(duration / 1000, 1),
                    outBytes / Math.max(duration / 1000, 1),
                    0L, 0L, 0L, 0L,
                    inBytes * 8 / Math.max(duration / 1000, 1),
                    outBytes * 8 / Math.max(duration / 1000, 1),
                    inPackets / 5, inPackets / 5, inPackets / 5, inPackets / 5, inPackets / 5,
                    65535, 65535,
                    protocol == 1 ? 8 : 0,
                    protocol == 1 ? 8 : 0,
                    protocol == 17 ? 100 + recordIdx : 0,
                    protocol == 17 ? 1 : 0,
                    protocol == 17 ? 300L : 0L,
                    protocol == 6 ? 200 : 0,
                    processTimeMicros
            );

            rows.add(row);
        }

        return rows;
    }

    public static void writeFiles(
            SparkSession spark,
            Path basePath,
            int numFiles,
            int recordsPerFile,
            Instant baseTimestamp) {

        StructType schema = getNetflowSchema();

        for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
            int partition = fileIdx % 3;
            List<Row> rows = generateRowsForFile(fileIdx, recordsPerFile, baseTimestamp, partition);

            Dataset<Row> df = spark.createDataFrame(rows, schema);

            Instant fileTimestamp = baseTimestamp.plusSeconds(fileIdx * 30L);
            String timestampStr = FILENAME_TIMESTAMP_FORMATTER.format(fileTimestamp);

            String uuid = UUID.nameUUIDFromBytes(
                    ("file-" + fileIdx + "-p" + partition).getBytes()
            ).toString();

            String fileName = String.format("netflow-p%02d-%s-%s.parquet", partition, timestampStr, uuid);
            String dateDirectory = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC).format(fileTimestamp);
            String hourDirectory = DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC).format(fileTimestamp);
            Path partitionPath = new Path(new Path(basePath, dateDirectory), hourDirectory);
            Path stagingPath = new Path(basePath, ".staging-" + uuid);

            df.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .parquet(stagingPath.toString());

            try {
                org.apache.hadoop.fs.FileSystem fileSystem = stagingPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
                fileSystem.mkdirs(partitionPath);
                FileStatus partFile = java.util.Arrays.stream(fileSystem.listStatus(stagingPath))
                        .filter(status -> status.getPath().getName().startsWith("part-"))
                        .findFirst()
                        .orElseThrow();
                if (!fileSystem.rename(partFile.getPath(), new Path(partitionPath, fileName))) {
                    throw new IllegalStateException("Failed to publish test Parquet file: " + fileName);
                }
                fileSystem.delete(stagingPath, true);
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Failed to publish test Parquet file: " + fileName, e);
            }

            log.debug("Generated test file: {} with {} records", fileName, recordsPerFile);
        }

        log.info("Successfully generated {} deterministic Parquet files in: {}", numFiles, basePath);
    }

    public static String writeFilesToTempDir(
            SparkSession spark,
            int numFiles,
            int recordsPerFile,
            Instant baseTimestamp) {

        String tempDir = System.getProperty("java.io.tmpdir") + "/nimbo-test/parquet";
        Path basePath = new Path(tempDir);
        writeFiles(spark, basePath, numFiles, recordsPerFile, baseTimestamp);
        return tempDir;
    }
}
