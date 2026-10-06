package com.nimbo.cubemaker.writer;

import static com.nimbo.cubemaker.constants.ColumnConstants.COL_CONNECTION_COUNT;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_DST_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_ID;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_INBOUND_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_INBOUND_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_IN_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_L7_PROTOCOL;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUTBOUND_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUTBOUND_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUT_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_RECORD_COUNT;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_SRC_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_WINDOW_END;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_WINDOW_START;

import com.nimbo.cubemaker.model.ConnectionAggregate;
import com.nimbo.cubemaker.model.ProtocolAggregate;
import com.nimbo.cubemaker.model.TopIpAggregate;
import com.nimbo.cubemaker.model.TrafficAggregate;
import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Component
public class TableRowMapper implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  public TrafficAggregate toTrafficAggregate(Row row, UUID batchId, Timestamp ingestedAt) {
    return TrafficAggregate.builder()
        .srcIp(row.getString(row.fieldIndex(COL_SRC_IP)))
        .windowStart(row.getTimestamp(row.fieldIndex(COL_WINDOW_START)).toLocalDateTime())
        .windowEnd(row.getTimestamp(row.fieldIndex(COL_WINDOW_END)).toLocalDateTime())
        .inBytes(row.getLong(row.fieldIndex(COL_IN_BYTES)))
        .outBytes(row.getLong(row.fieldIndex(COL_OUT_BYTES)))
        .inPackets(row.getLong(row.fieldIndex(COL_IN_PACKETS)))
        .outPackets(row.getLong(row.fieldIndex(COL_OUT_PACKETS)))
        .recordCount(row.getLong(row.fieldIndex(COL_RECORD_COUNT)))
        .batchId(batchId)
        .ingestedAt(ingestedAt.toLocalDateTime())
        .build();
  }

  public ConnectionAggregate toConnectionAggregate(Row row, UUID batchId, Timestamp ingestedAt) {
    return ConnectionAggregate.builder()
        .id(row.getString(row.fieldIndex(COL_ID)))
        .srcIp(row.getString(row.fieldIndex(COL_SRC_IP)))
        .dstIp(row.getString(row.fieldIndex(COL_DST_IP)))
        .windowStart(row.getTimestamp(row.fieldIndex(COL_WINDOW_START)).toLocalDateTime())
        .windowEnd(row.getTimestamp(row.fieldIndex(COL_WINDOW_END)).toLocalDateTime())
        .inBytes(row.getLong(row.fieldIndex(COL_IN_BYTES)))
        .outBytes(row.getLong(row.fieldIndex(COL_OUT_BYTES)))
        .inPackets(row.getLong(row.fieldIndex(COL_IN_PACKETS)))
        .outPackets(row.getLong(row.fieldIndex(COL_OUT_PACKETS)))
        .batchId(batchId)
        .ingestedAt(ingestedAt.toLocalDateTime())
        .build();
  }

  public ProtocolAggregate toProtocolAggregate(Row row, UUID batchId, Timestamp ingestedAt) {
    return ProtocolAggregate.builder()
        .l7Protocol(row.getDouble(row.fieldIndex(COL_L7_PROTOCOL)))
        .windowStart(row.getTimestamp(row.fieldIndex(COL_WINDOW_START)).toLocalDateTime())
        .windowEnd(row.getTimestamp(row.fieldIndex(COL_WINDOW_END)).toLocalDateTime())
        .inBytes(row.getLong(row.fieldIndex(COL_IN_BYTES)))
        .outBytes(row.getLong(row.fieldIndex(COL_OUT_BYTES)))
        .inPackets(row.getLong(row.fieldIndex(COL_IN_PACKETS)))
        .outPackets(row.getLong(row.fieldIndex(COL_OUT_PACKETS)))
        .connectionCount(row.getLong(row.fieldIndex(COL_CONNECTION_COUNT)))
        .batchId(batchId)
        .ingestedAt(ingestedAt.toLocalDateTime())
        .build();
  }

  public TopIpAggregate toIpBandwidthAggregate(Row row, UUID batchId, Timestamp ingestedAt) {
    return TopIpAggregate.builder()
        .ip(row.getString(row.fieldIndex(COL_IP)))
        .windowStart(row.getTimestamp(row.fieldIndex(COL_WINDOW_START)).toLocalDateTime())
        .windowEnd(row.getTimestamp(row.fieldIndex(COL_WINDOW_END)).toLocalDateTime())
        .inboundBytes(row.getLong(row.fieldIndex(COL_INBOUND_BYTES)))
        .outboundBytes(row.getLong(row.fieldIndex(COL_OUTBOUND_BYTES)))
        .inboundPackets(row.getLong(row.fieldIndex(COL_INBOUND_PACKETS)))
        .outboundPackets(row.getLong(row.fieldIndex(COL_OUTBOUND_PACKETS)))
        .batchId(batchId)
        .ingestedAt(ingestedAt.toLocalDateTime())
        .build();
  }
}
