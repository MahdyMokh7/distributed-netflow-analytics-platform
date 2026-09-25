package com.nimbo.restapi.integration.service;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.request.ConnectionQueryRequestDto;
import com.nimbo.restapi.dto.request.PaginationRequestDto;
import com.nimbo.restapi.dto.request.ProtocolQueryRequestDto;
import com.nimbo.restapi.dto.request.TrafficQueryRequestDto;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.model.ConnectionAggregate;
import com.nimbo.restapi.model.IpBandwidthAggregate;
import com.nimbo.restapi.model.ProtocolUsage;
import com.nimbo.restapi.model.TrafficAggregate;
import com.nimbo.restapi.repository.BandwidthRepository;
import com.nimbo.restapi.repository.ConnectionRepository;
import com.nimbo.restapi.repository.ProtocolRepository;
import com.nimbo.restapi.repository.TrafficRepository;
import com.nimbo.restapi.service.impl.BandwidthQueryServiceImpl;
import com.nimbo.restapi.service.impl.ConnectionQueryServiceImpl;
import com.nimbo.restapi.service.impl.ProtocolQueryServiceImpl;
import com.nimbo.restapi.service.impl.TrafficQueryServiceImpl;
import com.nimbo.restapi.support.PostgresTestExtension;
import com.nimbo.restapi.util.QueryMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigInteger;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(PostgresTestExtension.class)
@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.jpa.properties.hibernate.jdbc.time_zone=UTC"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = PostgresTestExtension.DatasourceInitializer.class)
class PostgresQueryIT {

    private static final Instant RANGE_START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant RANGE_END = Instant.parse("2026-01-01T01:00:00Z");
    private static final UUID BATCH_ONE = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID BATCH_TWO = UUID.fromString("10000000-0000-0000-0000-000000000002");

    private final TrafficRepository trafficRepository;
    private final ConnectionRepository connectionRepository;
    private final BandwidthRepository bandwidthRepository;
    private final ProtocolRepository protocolRepository;
    private final JdbcTemplate jdbcTemplate;
    private final QueryMetrics metrics = new QueryMetrics(new SimpleMeterRegistry());

    @Autowired
    PostgresQueryIT(
            TrafficRepository trafficRepository,
            ConnectionRepository connectionRepository,
            BandwidthRepository bandwidthRepository,
            ProtocolRepository protocolRepository,
            JdbcTemplate jdbcTemplate
    ) {
        this.trafficRepository = trafficRepository;
        this.connectionRepository = connectionRepository;
        this.bandwidthRepository = bandwidthRepository;
        this.protocolRepository = protocolRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @BeforeEach
    void cleanDatabase() {
        jdbcTemplate.update("DELETE FROM traffic_aggregates");
        jdbcTemplate.update("DELETE FROM connection_aggregates");
        jdbcTemplate.update("DELETE FROM ip_bandwidth_aggregates");
        jdbcTemplate.update("DELETE FROM protocol_usage_aggregates");
    }

    @Test
    void aggregateSchemaMatchesProductionIdentityAndInetColumns() {
        Integer obsoleteIdColumns = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN (
                    'traffic_aggregates',
                    'ip_bandwidth_aggregates',
                    'protocol_usage_aggregates'
                  )
                  AND column_name = 'id'
                """, Integer.class);

        var inetColumns = jdbcTemplate.queryForList("""
                SELECT table_name || '.' || column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND udt_name = 'inet'
                ORDER BY table_name, column_name
                """, String.class);

        assertThat(obsoleteIdColumns).isZero();
        assertThat(inetColumns).containsExactly(
                "connection_aggregates.dst_ip",
                "connection_aggregates.src_ip",
                "ip_bandwidth_aggregates.ip",
                "traffic_aggregates.src_ip"
        );
    }

    @Test
    void givenTrafficAcrossWindowsWhenQueriedBySourceThenRangeSortAndTotalsAreCorrect() {
        // Given: traffic inside and at the exclusive range boundary
        trafficRepository.save(traffic("10.0.0.1", RANGE_START.plusSeconds(600), 100, 40, 10, 4, BATCH_ONE));
        trafficRepository.save(traffic("10.0.0.1", RANGE_START.plusSeconds(1200), 300, 20, 30, 2, BATCH_TWO));
        trafficRepository.save(traffic("10.0.0.1", RANGE_END, 900, 900, 90, 90, BATCH_ONE));

        // When: source traffic is requested in descending byte order
        var result = new TrafficQueryServiceImpl(trafficRepository, metrics).queryTraffic(
                TrafficQueryRequestDto.builder()
                        .srcIp("10.0.0.1")
                        .startTime(RANGE_START)
                        .endTime(RANGE_END)
                        .pagination(pagination("inBytes", "DESC", 0, 10))
                        .build()
        );

        // Then: only in-range rows are returned with calculated totals
        assertThat(result).hasSize(2);
        assertThat(result.getContent().get(0).getInBytes()).isEqualTo(BigInteger.valueOf(300));
        assertThat(result.getContent().get(0).getTotalBytes()).isEqualTo(BigInteger.valueOf(320));
        assertThat(result.getContent().get(0).getTotalPackets()).isEqualTo(BigInteger.valueOf(32));
        assertThat(result.getTotalElements()).isEqualTo(2);
    }

    @Test
    void givenSimplifiedConnectionsWhenQueriedByEitherIpThenBothDirectionsAreReturned() {
        // Given: bidirectional connections and an unrelated connection
        String firstId = "a1".repeat(32);
        String secondId = "a2".repeat(32);
        connectionRepository.save(connection(firstId, "10.0.0.1", "10.0.0.2", RANGE_START.plusSeconds(60), 120, 30, BATCH_ONE));
        connectionRepository.save(connection(secondId, "10.0.0.3", "10.0.0.1", RANGE_START.plusSeconds(120), 80, 20, BATCH_TWO));
        connectionRepository.save(connection("a3".repeat(32), "10.0.0.4", "10.0.0.5", RANGE_START.plusSeconds(180), 500, 50, BATCH_ONE));

        // When: connections involving one IP are requested
        var result = new ConnectionQueryServiceImpl(connectionRepository, metrics).queryConnectionsByIp(
                "10.0.0.1",
                ConnectionQueryRequestDto.builder()
                        .startTime(RANGE_START)
                        .endTime(RANGE_END)
                        .pagination(pagination("windowStart", "ASC", 0, 10))
                        .build()
        );

        // Then: source and destination matches use the simplified cube shape
        assertThat(result).hasSize(2);
        assertThat(result.getContent()).extracting("id").containsExactly(firstId, secondId);
        assertThat(result.getContent().get(0).getTotalBytes()).isEqualTo(BigInteger.valueOf(150));
        assertThat(result.getContent().get(0).getTotalPackets()).isEqualTo(BigInteger.valueOf(37));
    }

    @Test
    void givenBandwidthWindowsWhenTopIpsQueriedThenMetricsAreSummedAndRanked() {
        // Given: two IPs with deterministic inbound and outbound measurements
        bandwidthRepository.save(bandwidth("10.0.0.1", RANGE_START.plusSeconds(60), 100, 50, 10, 5, BATCH_ONE));
        bandwidthRepository.save(bandwidth("10.0.0.1", RANGE_START.plusSeconds(120), 200, 100, 20, 10, BATCH_TWO));
        bandwidthRepository.save(bandwidth("10.0.0.2", RANGE_START.plusSeconds(180), 400, 10, 40, 1, BATCH_ONE));

        // When: top bandwidth consumers are queried
        var result = new BandwidthQueryServiceImpl(bandwidthRepository, metrics).getTopBandwidthIps(
                BandwidthQueryRequestDto.builder()
                        .startTime(RANGE_START)
                        .endTime(RANGE_END)
                        .limit(2)
                        .build()
        );

        // Then: ranking and every aggregate column retain their meaning
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getIp()).isEqualTo("10.0.0.1");
        assertThat(result.get(0).getInboundBytes()).isEqualTo(BigInteger.valueOf(300));
        assertThat(result.get(0).getOutboundBytes()).isEqualTo(BigInteger.valueOf(150));
        assertThat(result.get(0).getInboundPackets()).isEqualTo(BigInteger.valueOf(30));
        assertThat(result.get(0).getOutboundPackets()).isEqualTo(BigInteger.valueOf(15));
        assertThat(result.get(0).getTotalPackets()).isEqualTo(BigInteger.valueOf(45));
        assertThat(result.get(0).getTotalBytes()).isEqualTo(BigInteger.valueOf(450));
    }

    @Test
    void givenProtocolWindowsWhenFilteredThenOnlyRequestedProtocolIsMapped() {
        // Given: two protocol aggregates in the same time range
        protocolRepository.save(protocol(6.0, RANGE_START.plusSeconds(60), 500, 100, 50, 10, 7, BATCH_ONE));
        protocolRepository.save(protocol(17.0, RANGE_START.plusSeconds(120), 200, 300, 20, 30, 4, BATCH_TWO));

        // When: one application protocol is requested
        var result = new ProtocolQueryServiceImpl(protocolRepository, metrics).queryProtocolUsage(
                ProtocolQueryRequestDto.builder()
                        .l7Protocol(17.0)
                        .startTime(RANGE_START)
                        .endTime(RANGE_END)
                        .build()
        );

        // Then: its persisted metrics and calculated totals are returned
        assertThat(result).singleElement().satisfies(response -> {
            assertThat(response.getL7Protocol()).isEqualTo(17.0);
            assertThat(response.getTotalBytes()).isEqualTo(BigInteger.valueOf(500));
            assertThat(response.getTotalPackets()).isEqualTo(BigInteger.valueOf(50));
            assertThat(response.getConnectionCount()).isEqualTo(4);
        });
    }

    @Test
    void givenNoMatchingTrafficWhenQueriedThenNotFoundIsReported() {
        // Given: an empty traffic table
        TrafficQueryRequestDto request = TrafficQueryRequestDto.builder()
                .startTime(RANGE_START)
                .endTime(RANGE_END)
                .pagination(pagination("windowStart", "DESC", 0, 10))
                .build();

        // When: the empty range is queried
        var invocation = org.assertj.core.api.Assertions.catchThrowable(
                () -> new TrafficQueryServiceImpl(trafficRepository, metrics).queryTraffic(request)
        );

        // Then: absence is distinguished from a successful empty response
        assertThat(invocation).isInstanceOf(ResourceNotFoundException.class)
                .hasMessage("No traffic data found for the given criteria");
    }

    @Test
    void givenUnsupportedSortFieldWhenQueriedThenItIsRejectedBeforeDatabaseAccess() {
        // Given: a request attempting to sort by a non-entity field
        ConnectionQueryRequestDto request = ConnectionQueryRequestDto.builder()
                .startTime(RANGE_START)
                .endTime(RANGE_END)
                .pagination(pagination("srcPort", "DESC", 0, 10))
                .build();

        // When: the unsafe sort field is processed
        var invocation = org.assertj.core.api.Assertions.catchThrowable(
                () -> new ConnectionQueryServiceImpl(connectionRepository, metrics).queryConnections(request)
        );

        // Then: the removed connection field cannot reach query construction
        assertThat(invocation).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported connection sort field: srcPort");
    }

    private PaginationRequestDto pagination(String sortBy, String direction, int page, int size) {
        return PaginationRequestDto.builder()
                .page(page)
                .size(size)
                .sortBy(sortBy)
                .sortDirection(direction)
                .build();
    }

    private TrafficAggregate traffic(
            String srcIp, Instant start, long inBytes, long outBytes,
            long inPackets, long outPackets, UUID batchId
    ) {
        return TrafficAggregate.builder()
                .srcIp(srcIp).windowStart(start).windowEnd(start.plusSeconds(300))
                .inBytes(number(inBytes)).outBytes(number(outBytes))
                .inPackets(number(inPackets)).outPackets(number(outPackets))
                .recordCount(BigInteger.ONE).batchId(batchId).ingestedAt(start.plusSeconds(310))
                .build();
    }

    private ConnectionAggregate connection(
            String id, String srcIp, String dstIp, Instant start,
            long bytes, long packets, UUID batchId
    ) {
        return ConnectionAggregate.builder()
                .id(HexFormat.of().parseHex(id)).srcIp(srcIp).dstIp(dstIp).windowStart(start).windowEnd(start.plusSeconds(300))
                .inBytes(number(bytes)).outBytes(number(bytes / 4))
                .inPackets(number(packets)).outPackets(number(packets / 4))
                .batchId(batchId).ingestedAt(start.plusSeconds(310))
                .build();
    }

    private IpBandwidthAggregate bandwidth(
            String ip, Instant start, long inboundBytes, long outboundBytes,
            long inboundPackets, long outboundPackets, UUID batchId
    ) {
        return IpBandwidthAggregate.builder()
                .ip(ip).windowStart(start).windowEnd(start.plusSeconds(300))
                .inboundBytes(number(inboundBytes)).outboundBytes(number(outboundBytes))
                .inboundPackets(number(inboundPackets)).outboundPackets(number(outboundPackets))
                .batchId(batchId).ingestedAt(start.plusSeconds(310))
                .build();
    }

    private ProtocolUsage protocol(
            double protocol, Instant start, long inBytes, long outBytes,
            long inPackets, long outPackets, long connections, UUID batchId
    ) {
        return ProtocolUsage.builder()
                .l7Protocol(protocol).windowStart(start).windowEnd(start.plusSeconds(300))
                .inBytes(number(inBytes)).outBytes(number(outBytes))
                .inPackets(number(inPackets)).outPackets(number(outPackets))
                .connectionCount(connections).batchId(batchId).ingestedAt(start.plusSeconds(310))
                .build();
    }

    private BigInteger number(long value) {
        return BigInteger.valueOf(value);
    }
}
