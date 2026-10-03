package com.nimbo.backfill.sql;

import static com.nimbo.backfill.config.BackfillConfigTest.validEnvironment;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.BackfillProjection.HdfsPathProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.service.BackfillPlanner;

class NetflowBackfillSqlTest {

    private BackfillConfig config;
    private DailyBackfillPlan plan;
    private BackfillProjection projection;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);
        config = BackfillConfig.fromEnvironment(validEnvironment(), clock);
        plan = new BackfillPlanner().createPlan(config);
        projection = new BackfillProjection(List.of("ipv4_src_addr", "kafka_timestamp", "kafka_topic"),
                List.of(load(plan.hdfsPaths().get(0)), load(plan.hdfsPaths().get(1))), List.of());
    }

    @Test
    void createsAnExplicitParquetToStarRocksMapping() {
        String sql = NetflowBackfillSql.loadHdfs(config, plan, projection);

        assertThat(sql).contains("TEMPORARY PARTITION").contains("`ipv4_src_addr`").contains("`IPV4_SRC_ADDR`")
                .contains("CAST(PROCESS_TIME / 1000 AS BIGINT)").contains("`KAFKA_TOPIC`")
                .contains("PROCESS_TIME >= " + plan.fromMicros()).contains("PROCESS_TIME < " + plan.toMicros())
                .doesNotContain("hdfs-backfill").doesNotContain("SELECT *");
    }

    @Test
    void createsADailyContainerButStagesOnlyTheRequestedInterval() {
        assertThat(NetflowBackfillSql.createTemporaryPartition(config, plan)).contains("ADD TEMPORARY PARTITION")
                .contains("2026-09-26 00:00:00").contains("VALUES IN (('2026-09-26 00:00:00'))")
                .doesNotContain("2026-09-27 00:00:00");

        assertThat(NetflowBackfillSql.loadHdfs(config, plan, projection))
                .contains("PROCESS_TIME >= " + plan.fromMicros()).contains("PROCESS_TIME < " + plan.toMicros());
    }

    @Test
    void deletesAndInsertsOnlyTheExactInterval() {
        assertThat(NetflowBackfillSql.deleteFormalInterval(config, plan)).contains("DELETE FROM `nimbo`.`netflow`")
                .doesNotContain("PARTITION")
                .contains("kafka_timestamp >= " + plan.fromMillis()).contains("kafka_timestamp < " + plan.toMillis());

        assertThat(NetflowBackfillSql.copyTemporaryIntervalIntoFormal(config, plan, projection))
                .contains("INSERT INTO `nimbo`.`netflow` (")
                .doesNotContain("INSERT INTO `nimbo`.`netflow` PARTITION")
                .contains("TEMPORARY PARTITION (`" + plan.temporaryPartition() + "`)")
                .contains("kafka_timestamp >= " + plan.fromMillis()).contains("kafka_timestamp < " + plan.toMillis());
    }

    @Test
    void measuresTheFormalIntervalWithoutNamingAPartition() {
        assertThat(NetflowBackfillSql.formalIntervalMetrics(config, plan)).doesNotContain("PARTITION (");
    }

    @Test
    void validatesTheFullProjectionWithoutWriting() {
        assertThat(NetflowBackfillSql.validateHdfsProjection(config, plan, projection.pathProjections().get(0)))
                .contains("`IPV4_SRC_ADDR`").contains("CAST(PROCESS_TIME / 1000 AS BIGINT)").endsWith("LIMIT 1");
    }

    @Test
    void discoversBothSchemasThroughStarRocks() {
        assertThat(NetflowBackfillSql.describeTable(config)).isEqualTo("DESC `nimbo`.`netflow`");
        assertThat(NetflowBackfillSql.describeHdfs(config, plan.hdfsPaths().get(0))).startsWith("DESC FILES(")
                .contains("fill_mismatch_column_with").contains("auto_detect_sample_files");
    }

    private HdfsPathProjection load(String path) {
        return new HdfsPathProjection(path,
                List.of("CAST(`IPV4_SRC_ADDR` AS varchar(15))", "CAST(PROCESS_TIME / 1000 AS BIGINT)",
                        "CAST(`KAFKA_TOPIC` AS varchar(255))"),
                "mapping");
    }
}
