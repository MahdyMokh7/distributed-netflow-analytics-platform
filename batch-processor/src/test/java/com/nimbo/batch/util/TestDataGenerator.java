package com.nimbo.batch.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class TestDataGenerator {

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

    private static final int[] PROTOCOLS = {
            6,   // TCP
            17,  // UDP
            1,   // ICMP
            2    // IGMP
    };

    private TestDataGenerator() {
    }

    public static StructType getNetflowSchema() {
        return DataTypes.createStructType(new StructField[]{

                DataTypes.createStructField(
                        "IPV4_SRC_ADDR",
                        DataTypes.StringType,
                        false
                ),

                DataTypes.createStructField(
                        "IPV4_DST_ADDR",
                        DataTypes.StringType,
                        false
                ),

                DataTypes.createStructField(
                        "L4_SRC_PORT",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "L4_DST_PORT",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "PROTOCOL",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "L7_PROTO",
                        DataTypes.DoubleType,
                        false
                ),

                DataTypes.createStructField(
                        "IN_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "OUT_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "IN_PKTS",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "OUT_PKTS",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "FLOW_DURATION_MILLISECONDS",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "TCP_FLAGS",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "CLIENT_TCP_FLAGS",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "SERVER_TCP_FLAGS",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "DURATION_IN",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "DURATION_OUT",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "MIN_TTL",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "MAX_TTL",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "LONGEST_FLOW_PKT",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "SHORTEST_FLOW_PKT",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "MIN_IP_PKT_LEN",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "MAX_IP_PKT_LEN",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "SRC_TO_DST_SECOND_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "DST_TO_SRC_SECOND_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "RETRANSMITTED_IN_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "RETRANSMITTED_IN_PKTS",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "RETRANSMITTED_OUT_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "RETRANSMITTED_OUT_PKTS",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "SRC_TO_DST_AVG_THROUGHPUT",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "DST_TO_SRC_AVG_THROUGHPUT",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "NUM_PKTS_UP_TO_128_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "NUM_PKTS_128_TO_256_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "NUM_PKTS_256_TO_512_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "NUM_PKTS_512_TO_1024_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "NUM_PKTS_1024_TO_1514_BYTES",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "TCP_WIN_MAX_IN",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "TCP_WIN_MAX_OUT",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "ICMP_TYPE",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "ICMP_IPV4_TYPE",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "DNS_QUERY_ID",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "DNS_QUERY_TYPE",
                        DataTypes.IntegerType,
                        false
                ),

                DataTypes.createStructField(
                        "DNS_TTL_ANSWER",
                        DataTypes.LongType,
                        false
                ),

                DataTypes.createStructField(
                        "FTP_COMMAND_RET_CODE",
                        DataTypes.IntegerType,
                        false
                ),

                // Added PROCESSING_TIME column
                DataTypes.createStructField(
                        "PROCESSING_TIME",
                        DataTypes.TimestampType,
                        true
                )
        });
    }

    public static List<Row> generateSampleRows(int count) {
        List<Row> rows = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            rows.add(createRow(i));
        }

        return rows;
    }

    private static Row createRow(int index) {

        String srcIp =
                IP_ADDRESSES[index % IP_ADDRESSES.length];

        String dstIp =
                IP_ADDRESSES[(index + 1) % IP_ADDRESSES.length];

        int srcPort =
                1024 + (index % 60000);

        int dstPort =
                1024 + ((index * 2) % 60000);

        int protocol =
                PROTOCOLS[index % PROTOCOLS.length];

        double l7Proto =
                switch (protocol) {
                    case 6 -> 6.0;    // TCP
                    case 17 -> 17.0;  // UDP
                    case 1 -> 1.0;    // ICMP
                    default -> 2.0;   // IGMP
                };

        long inBytes =
                1000L + (index * 100L);

        long outBytes =
                500L + (index * 50L);

        long inPackets =
                10L + index;

        long outPackets =
                5L + index;

        long duration =
                100L + (index % 1000);

        int minTtl = 32;
        int maxTtl = 64;

        int shortestPacket = 64;
        int longestPacket = 1500;

        // Generate a timestamp within the last hour
        Timestamp processingTime = Timestamp.from(Instant.now());

        return RowFactory.create(

                // 1-2
                srcIp,
                dstIp,

                // 3-5
                srcPort,
                dstPort,
                protocol,

                // 6
                l7Proto,

                // 7-10
                inBytes,
                outBytes,
                inPackets,
                outPackets,

                // 11
                duration,

                // 12-14
                16,
                8,
                8,

                // 15-16
                duration / 2,
                duration / 2,

                // 17-18
                minTtl,
                maxTtl,

                // 19-22
                longestPacket,
                shortestPacket,
                shortestPacket,
                longestPacket,

                // 23-24
                inBytes / Math.max(duration / 1000, 1),
                outBytes / Math.max(duration / 1000, 1),

                // 25-28
                0L,
                0L,
                0L,
                0L,

                // 29-30
                inBytes * 8 / Math.max(duration / 1000, 1),
                outBytes * 8 / Math.max(duration / 1000, 1),

                // 31-35
                inPackets / 5,
                inPackets / 5,
                inPackets / 5,
                inPackets / 5,
                inPackets / 5,

                // 36-37
                65535,
                65535,

                // 38-39
                protocol == 1 ? 8 : 0,
                protocol == 1 ? 8 : 0,

                // 40-41
                protocol == 17 ? 100 + index : 0,
                protocol == 17 ? 1 : 0,

                // 42
                protocol == 17 ? 300L : 0L,

                // 43
                protocol == 6 ? 200 : 0,

                // 44 - PROCESSING_TIME
                processingTime
        );
    }

    public static void writeSampleParquetFiles(
            SparkSession spark,
            String outputPath,
            int recordsPerFile,
            int filesPerPartition
    ) {
        StructType schema = getNetflowSchema();
        int totalPartitions = 3;
        int totalFiles = totalPartitions * filesPerPartition;

        List<Row> allRows = new ArrayList<>();
        for (int i = 0; i < totalFiles; i++) {
            allRows.addAll(generateSampleRows(recordsPerFile));
        }

        Dataset<Row> df = spark.createDataFrame(allRows, schema);

        df.repartition(totalFiles)
                .write()
                .mode("overwrite")
                .parquet(outputPath);

        System.out.println("Created " + totalFiles + " Parquet part-files under: " + outputPath);
    }
}