package com.nimbo.backfill.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.HdfsSchema;
import com.nimbo.backfill.model.SchemaColumn;

class BackfillSchemaMapperTest {

    private static final String TABLE = "`nimbo`.`netflow`";
    private static final String PATH = "hdfs://namenode/netflow/2026-09-26/10/*.parquet";

    private final BackfillSchemaMapper mapper = new BackfillSchemaMapper();

    @Test
    void rejectsParquetWithoutRequiredKafkaMetadata() {
        assertThatThrownBy(() -> mapper.map(TABLE, targetColumns(), List.of(oldHdfsSchema()), List.of()))
                .isInstanceOf(BackfillException.class)
                .hasMessageContaining("missing required StarRocks column kafka_topic");
    }

    @Test
    void usesKafkaMetadataWhenNewParquetContainsIt() {
        HdfsSchema schema = new HdfsSchema(PATH,
                List.of(column("IPV4_SRC_ADDR", "varchar(1048576)", true), column("PROCESS_TIME", "bigint", true),
                        column("KAFKA_TOPIC", "varchar(1048576)", true), column("KAFKA_PARTITION", "int", true),
                        column("KAFKA_OFFSET", "bigint", true)));

        BackfillProjection projection = mapper.map(TABLE, targetColumns(), List.of(schema), List.of());

        assertThat(projection.pathProjections().get(0).expressions()).contains("CAST(`KAFKA_TOPIC` AS varchar(255))",
                "CAST(`KAFKA_PARTITION` AS int)", "CAST(`KAFKA_OFFSET` AS bigint)")
                .noneMatch(value -> value.startsWith("COALESCE"));
    }

    @Test
    void fillsNullableStarRocksOnlyColumnWithNull() {
        List<SchemaColumn> target =
                List.of(column("kafka_timestamp", "bigint", false), column("optional_v2", "varchar(20)", true));
        HdfsSchema source = new HdfsSchema(PATH, List.of(column("PROCESS_TIME", "bigint", true)));

        BackfillProjection projection = mapper.map(TABLE, target, List.of(source), List.of());

        assertThat(projection.pathProjections().get(0).expressions())
                .containsExactly("CAST(`PROCESS_TIME` / 1000 AS bigint)", "CAST(NULL AS varchar(20))");
    }

    @Test
    void explainsHowToAddAnHdfsOnlyColumnToStarRocks() {
        HdfsSchema source = new HdfsSchema(PATH,
                List.of(column("PROCESS_TIME", "bigint", true), column("NEW_FIELD", "bigint", true)));

        assertThatThrownBy(() -> mapper.map(TABLE, List.of(column("kafka_timestamp", "bigint", false)), List.of(source),
                List.of())).isInstanceOf(BackfillException.class)
                .hasMessageContaining("StarRocks is missing HDFS column(s) NEW_FIELD")
                .hasMessageContaining("ALTER TABLE `nimbo`.`netflow` ADD COLUMN `new_field` bigint NULL;");
    }

    @Test
    void rejectsMissingRequiredTargetColumn() {
        List<SchemaColumn> target =
                List.of(column("kafka_timestamp", "bigint", false), column("required_v2", "int", false));
        HdfsSchema source = new HdfsSchema(PATH, List.of(column("PROCESS_TIME", "bigint", true)));

        assertThatThrownBy(() -> mapper.map(TABLE, target, List.of(source), List.of()))
                .isInstanceOf(BackfillException.class)
                .hasMessageContaining("missing required StarRocks column required_v2")
                .hasMessageContaining("MODIFY COLUMN `required_v2` int NULL");
    }

    @Test
    void rejectsIncompatibleTypesBeforeWriting() {
        List<SchemaColumn> target =
                List.of(column("kafka_timestamp", "bigint", false), column("value", "bigint", false));
        HdfsSchema source = new HdfsSchema(PATH,
                List.of(column("PROCESS_TIME", "bigint", true), column("VALUE", "varchar(20)", true)));

        assertThatThrownBy(() -> mapper.map(TABLE, target, List.of(source), List.of()))
                .isInstanceOf(BackfillException.class).hasMessageContaining("Incompatible column types")
                .hasMessageContaining("VALUE is varchar(20)").hasMessageContaining("value is bigint");
    }

    @Test
    void rejectsLossyNumericConversionBeforeWriting() {
        List<SchemaColumn> target =
                List.of(column("kafka_timestamp", "bigint", false), column("value", "int", false));
        HdfsSchema source = new HdfsSchema(PATH,
                List.of(column("PROCESS_TIME", "bigint", true), column("VALUE", "bigint", true)));

        assertThatThrownBy(() -> mapper.map(TABLE, target, List.of(source), List.of()))
                .isInstanceOf(BackfillException.class).hasMessageContaining("Incompatible column types")
                .hasMessageContaining("VALUE is bigint").hasMessageContaining("value is int");
    }

    private List<SchemaColumn> targetColumns() {
        return List.of(column("ipv4_src_addr", "varchar(15)", false), column("kafka_timestamp", "bigint", false),
                column("kafka_topic", "varchar(255)", false), column("kafka_partition", "int", false),
                column("kafka_offset", "bigint", false));
    }

    private HdfsSchema oldHdfsSchema() {
        return new HdfsSchema(PATH,
                List.of(column("IPV4_SRC_ADDR", "varchar(1048576)", true), column("PROCESS_TIME", "bigint", true)));
    }

    private SchemaColumn column(String name, String type, boolean nullable) {
        return new SchemaColumn(name, type, nullable);
    }
}
