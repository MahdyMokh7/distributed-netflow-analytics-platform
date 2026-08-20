package com.nimbo.realtime.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.consumer.StarRocksConsumerWorker;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

@Component
public class StarRocksStreamLoadClient {

    private final StarRocksProperties properties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private static final Logger log =
            LoggerFactory.getLogger(StarRocksStreamLoadClient.class);

    private static final String COLUMNS =
            "kafka_topic,"
                    + "kafka_partition,"
                    + "kafka_offset,"
                    + "kafka_timestamp,"
                    + "ipv4_src_addr,"
                    + "ipv4_dst_addr,"
                    + "l4_src_port,"
                    + "l4_dst_port,"
                    + "protocol,"
                    + "l7_proto,"
                    + "in_bytes,"
                    + "out_bytes,"
                    + "in_pkts,"
                    + "out_pkts,"
                    + "flow_duration_milliseconds,"
                    + "tcp_flags,"
                    + "client_tcp_flags,"
                    + "server_tcp_flags,"
                    + "duration_in,"
                    + "duration_out,"
                    + "min_ttl,"
                    + "max_ttl,"
                    + "longest_flow_pkt,"
                    + "shortest_flow_pkt,"
                    + "min_ip_pkt_len,"
                    + "max_ip_pkt_len,"
                    + "src_to_dst_second_bytes,"
                    + "dst_to_src_second_bytes,"
                    + "retransmitted_in_bytes,"
                    + "retransmitted_in_pkts,"
                    + "retransmitted_out_bytes,"
                    + "retransmitted_out_pkts,"
                    + "src_to_dst_avg_throughput,"
                    + "dst_to_src_avg_throughput,"
                    + "num_pkts_up_to_128_bytes,"
                    + "num_pkts_128_to_256_bytes,"
                    + "num_pkts_256_to_512_bytes,"
                    + "num_pkts_512_to_1024_bytes,"
                    + "num_pkts_1024_to_1514_bytes,"
                    + "tcp_win_max_in,"
                    + "tcp_win_max_out,"
                    + "icmp_type,"
                    + "icmp_ipv4_type,"
                    + "dns_query_id,"
                    + "dns_query_type,"
                    + "dns_ttl_answer,"
                    + "ftp_command_ret_code";



    public StarRocksStreamLoadClient(
            StarRocksProperties properties,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.objectMapper = objectMapper;

        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    public StreamLoadResult load(
            List<StarRocksRow> rows,
            String label
    ) throws IOException, InterruptedException {

        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream Load batch cannot be empty"
            );
        }

        long serializationStart =
                System.nanoTime();

        String csvBody = serializeCsv(rows);

        long serializationMs =
                (System.nanoTime() - serializationStart)
                        / 1_000_000;

        long httpStart = System.nanoTime();

        HttpResponse<String> response =
                sendWithRedirect(
                        URI.create(buildStreamLoadUrl()),
                        csvBody,
                        label
                );

        if (response.statusCode() < 200
                || response.statusCode() >= 300) {

            throw new IOException(
                    "StarRocks Stream Load HTTP failure. status="
                            + response.statusCode()
                            + ", body="
                            + response.body()
            );
        }
        long httpMs =
                (System.nanoTime() - httpStart)
                        / 1_000_000;
        log.info(
                "StreamLoad client timing: rows={}, csv={}ms, http={}ms",
                rows.size(),
                serializationMs,
                httpMs
        );

        return objectMapper.readValue(
                response.body(),
                StreamLoadResult.class
        );
    }
    private HttpRequest buildRequest(
            URI uri,
            String csvBody,
            String label
    ) {

        HttpRequest.Builder builder =
                HttpRequest.newBuilder()
                        .uri(uri)
                        .header(
                                "Authorization",
                                buildAuthorizationHeader()
                        )
                        .header("format", "CSV")
                        .header("column_separator", ",")
                        .header("enclose", "\"")
                        .header("columns", COLUMNS)
                        .header("strict_mode", "true")
                        .header("max_filter_ratio", "0")
                        .header("Content-Type", "text/csv")
                        .expectContinue(true);

        if (properties.isMergeCommitEnabled()) {

            builder.header(
                    "enable_merge_commit",
                    "true"
            );

            builder.header(
                    "merge_commit_async",
                    Boolean.toString(
                            properties.isMergeCommitAsync()
                    )
            );

            builder.header(
                    "merge_commit_interval_ms",
                    Long.toString(
                            properties.getMergeCommitIntervalMs()
                    )
            );

            builder.header(
                    "merge_commit_parallel",
                    Integer.toString(
                            properties.getMergeCommitParallel()
                    )
            );

        } else {

            builder.header("label", label);
        }

        return builder
                .PUT(
                        HttpRequest.BodyPublishers.ofString(
                                csvBody,
                                StandardCharsets.UTF_8
                        )
                )
                .build();
    }

//    private String serialize(
//            List<StarRocksRow> rows
//    ) throws JsonProcessingException {
//
//        return objectMapper.writeValueAsString(rows);
//    }

    private String buildStreamLoadUrl() {

        String baseUrl =
                properties.getStreamLoadUrl()
                        .replaceAll("/+$", "");

        return baseUrl
                + "/api/"
                + properties.getDatabase()
                + "/"
                + properties.getTable()
                + "/_stream_load";
    }private void appendCsv(
            StringBuilder csv,
            String value
    ) {

        if (value == null) {
            csv.append("\\N");
            return;
        }

        csv.append('"');

        for (int i = 0; i < value.length(); i++) {

            char c = value.charAt(i);

            if (c == '"') {
                csv.append("\"\"");
            } else {
                csv.append(c);
            }
        }

        csv.append('"');
    }
    private String serializeCsv(
            List<StarRocksRow> rows
    ) {

        StringBuilder csv =
                new StringBuilder(rows.size() * 256);

        for (StarRocksRow row : rows) {

            appendCsv(csv, row.kafkaTopic());
            csv.append(',');

            csv.append(row.kafkaPartition()).append(',');
            csv.append(row.kafkaOffset()).append(',');
            csv.append(row.kafkaTimestamp()).append(',');

            appendCsv(csv, row.ipv4SrcAddr());
            csv.append(',');

            appendCsv(csv, row.ipv4DstAddr());
            csv.append(',');

            csv.append(row.l4SrcPort()).append(',');
            csv.append(row.l4DstPort()).append(',');

            csv.append(row.protocol()).append(',');
            csv.append(row.l7Proto()).append(',');

            csv.append(row.inBytes()).append(',');
            csv.append(row.outBytes()).append(',');

            csv.append(row.inPkts()).append(',');
            csv.append(row.outPkts()).append(',');

            csv.append(row.flowDurationMilliseconds()).append(',');

            csv.append(row.tcpFlags()).append(',');
            csv.append(row.clientTcpFlags()).append(',');
            csv.append(row.serverTcpFlags()).append(',');

            csv.append(row.durationIn()).append(',');
            csv.append(row.durationOut()).append(',');

            csv.append(row.minTtl()).append(',');
            csv.append(row.maxTtl()).append(',');

            csv.append(row.longestFlowPkt()).append(',');
            csv.append(row.shortestFlowPkt()).append(',');

            csv.append(row.minIpPktLen()).append(',');
            csv.append(row.maxIpPktLen()).append(',');

            csv.append(row.srcToDstSecondBytes()).append(',');
            csv.append(row.dstToSrcSecondBytes()).append(',');

            csv.append(row.retransmittedInBytes()).append(',');
            csv.append(row.retransmittedInPkts()).append(',');

            csv.append(row.retransmittedOutBytes()).append(',');
            csv.append(row.retransmittedOutPkts()).append(',');

            csv.append(row.srcToDstAvgThroughput()).append(',');
            csv.append(row.dstToSrcAvgThroughput()).append(',');

            csv.append(row.numPktsUpTo128Bytes()).append(',');
            csv.append(row.numPkts128To256Bytes()).append(',');
            csv.append(row.numPkts256To512Bytes()).append(',');
            csv.append(row.numPkts512To1024Bytes()).append(',');
            csv.append(row.numPkts1024To1514Bytes()).append(',');

            csv.append(row.tcpWinMaxIn()).append(',');
            csv.append(row.tcpWinMaxOut()).append(',');

            csv.append(row.icmpType()).append(',');
            csv.append(row.icmpIpv4Type()).append(',');

            csv.append(row.dnsQueryId()).append(',');
            csv.append(row.dnsQueryType()).append(',');
            csv.append(row.dnsTtlAnswer()).append(',');

            csv.append(row.ftpCommandRetCode());

            csv.append('\n');
        }

        return csv.toString();
    }
    private HttpResponse<String> sendWithRedirect(
            URI initialUri,
            String csvBody,
            String label
    ) throws IOException, InterruptedException {

        URI currentUri = initialUri;

        int maxRedirects = 5;

        for (int redirectCount = 0;
             redirectCount <= maxRedirects;
             redirectCount++) {

            HttpRequest request =
                    buildRequest(
                            currentUri,
                            csvBody,
                            label
                    );

            HttpResponse<String> response =
                    httpClient.send(
                            request,
                            HttpResponse.BodyHandlers.ofString(
                                    StandardCharsets.UTF_8
                            )
                    );

            if (!isRedirect(response.statusCode())) {
                return response;
            }

            String location =
                    response.headers()
                            .firstValue("Location")
                            .orElseThrow(
                                    () -> new IOException(
                                            "StarRocks returned HTTP "
                                                    + response.statusCode()
                                                    + " without Location header"
                                    )
                            );

            URI nextUri =
                    currentUri.resolve(location);

            log.info(
                    "StarRocks redirect: status={}, from={}, to={}",
                    response.statusCode(),
                    currentUri,
                    nextUri
            );

            if (nextUri.equals(currentUri)) {
                throw new IOException(
                        "StarRocks redirect loop detected: "
                                + currentUri
                );
            }

            currentUri = nextUri;
        }

        throw new IOException(
                "Too many StarRocks redirects. max="
                        + maxRedirects
        );
    }

    private boolean isRedirect(int statusCode) {
        return statusCode == 301
                || statusCode == 302
                || statusCode == 307
                || statusCode == 308;
    }
    private String buildAuthorizationHeader() {

        String credentials =
                properties.getUsername()
                        + ":"
                        + properties.getPassword();

        String encoded =
                Base64.getEncoder()
                        .encodeToString(
                                credentials.getBytes(
                                        StandardCharsets.UTF_8
                                )
                        );

        return "Basic " + encoded;
    }
}