package com.nimbo.batch.writer;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnTypeConstantsTest {

    @Test
    void shouldHaveAllTypeConstantsDefined() {
        assertThat(ColumnTypeConstants.STRING).isEqualTo("string");
        assertThat(ColumnTypeConstants.TIMESTAMP).isEqualTo("timestamp");
        assertThat(ColumnTypeConstants.BIGINT).isEqualTo("bigint");
        assertThat(ColumnTypeConstants.INTEGER).isEqualTo("integer");
        assertThat(ColumnTypeConstants.DOUBLE).isEqualTo("double");
        assertThat(ColumnTypeConstants.UUID).isEqualTo("uuid");
    }

    @Test
    void shouldHaveAllCommonColumnConstantsDefined() {
        assertThat(ColumnTypeConstants.COL_WINDOW_START).isEqualTo("window_start");
        assertThat(ColumnTypeConstants.COL_WINDOW_END).isEqualTo("window_end");
        assertThat(ColumnTypeConstants.COL_BATCH_ID).isEqualTo("batch_id");
        assertThat(ColumnTypeConstants.COL_INGESTED_AT).isEqualTo("ingested_at");
        assertThat(ColumnTypeConstants.COL_IN_BYTES).isEqualTo("in_bytes");
        assertThat(ColumnTypeConstants.COL_OUT_BYTES).isEqualTo("out_bytes");
        assertThat(ColumnTypeConstants.COL_IN_PACKETS).isEqualTo("in_packets");
        assertThat(ColumnTypeConstants.COL_OUT_PACKETS).isEqualTo("out_packets");
    }

    @Test
    void shouldHaveAllTableSpecificConstantsDefined() {
        assertThat(ColumnTypeConstants.COL_RECORD_COUNT).isEqualTo("record_count");
        assertThat(ColumnTypeConstants.COL_ID).isEqualTo("id");
        assertThat(ColumnTypeConstants.COL_SRC_IP).isEqualTo("src_ip");
        assertThat(ColumnTypeConstants.COL_DST_IP).isEqualTo("dst_ip");
        assertThat(ColumnTypeConstants.COL_SRC_PORT).isEqualTo("src_port");
        assertThat(ColumnTypeConstants.COL_DST_PORT).isEqualTo("dst_port");
        assertThat(ColumnTypeConstants.COL_L4_PROTOCOL).isEqualTo("l4_protocol");
        assertThat(ColumnTypeConstants.COL_L7_PROTOCOL).isEqualTo("l7_protocol");
    }

    @Test
    void shouldHaveAllBandwidthConstantsDefined() {
        assertThat(ColumnTypeConstants.COL_CONNECTION_COUNT).isEqualTo("connection_count");
        assertThat(ColumnTypeConstants.COL_IP).isEqualTo("ip");
        assertThat(ColumnTypeConstants.COL_INBOUND_BYTES).isEqualTo("inbound_bytes");
        assertThat(ColumnTypeConstants.COL_OUTBOUND_BYTES).isEqualTo("outbound_bytes");
        assertThat(ColumnTypeConstants.COL_INBOUND_PACKETS).isEqualTo("inbound_packets");
        assertThat(ColumnTypeConstants.COL_OUTBOUND_PACKETS).isEqualTo("outbound_packets");
    }

    @Test
    void shouldHavePrivateConstructor() throws Exception {
        Constructor<ColumnTypeConstants> constructor = ColumnTypeConstants.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        ColumnTypeConstants instance = constructor.newInstance();
        assertThat(instance).isNotNull();
    }

    @Test
    void shouldNotBeInstantiable() throws Exception {
        Constructor<ColumnTypeConstants> constructor = ColumnTypeConstants.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        assertThat(constructor).isNotNull();
    }

    @Test
    void shouldHaveNoDuplicatesInTypeConstants() {
        String[] types = {
                ColumnTypeConstants.STRING,
                ColumnTypeConstants.TIMESTAMP,
                ColumnTypeConstants.BIGINT,
                ColumnTypeConstants.INTEGER,
                ColumnTypeConstants.DOUBLE,
                ColumnTypeConstants.UUID
        };

        for (int i = 0; i < types.length; i++) {
            for (int j = i + 1; j < types.length; j++) {
                assertThat(types[i]).isNotEqualTo(types[j]);
            }
        }
    }

    @Test
    void shouldHaveNoDuplicatesInColumnConstants() {
        String[] columns = {
                ColumnTypeConstants.COL_WINDOW_START,
                ColumnTypeConstants.COL_WINDOW_END,
                ColumnTypeConstants.COL_BATCH_ID,
                ColumnTypeConstants.COL_INGESTED_AT,
                ColumnTypeConstants.COL_IN_BYTES,
                ColumnTypeConstants.COL_OUT_BYTES,
                ColumnTypeConstants.COL_IN_PACKETS,
                ColumnTypeConstants.COL_OUT_PACKETS,
                ColumnTypeConstants.COL_RECORD_COUNT,
                ColumnTypeConstants.COL_ID,
                ColumnTypeConstants.COL_SRC_IP,
                ColumnTypeConstants.COL_DST_IP,
                ColumnTypeConstants.COL_SRC_PORT,
                ColumnTypeConstants.COL_DST_PORT,
                ColumnTypeConstants.COL_L4_PROTOCOL,
                ColumnTypeConstants.COL_L7_PROTOCOL,
                ColumnTypeConstants.COL_CONNECTION_COUNT,
                ColumnTypeConstants.COL_IP,
                ColumnTypeConstants.COL_INBOUND_BYTES,
                ColumnTypeConstants.COL_OUTBOUND_BYTES,
                ColumnTypeConstants.COL_INBOUND_PACKETS,
                ColumnTypeConstants.COL_OUTBOUND_PACKETS
        };

        for (int i = 0; i < columns.length; i++) {
            for (int j = i + 1; j < columns.length; j++) {
                assertThat(columns[i]).isNotEqualTo(columns[j]);
            }
        }
    }
}