package com.nimbo.starrockswriter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.starrockswriter.model.StarRocksRow;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CsvFormatterTest {

  private final CsvFormatter formatter = new CsvFormatter();

  @Test
  void shouldReturnEmptyCsvForEmptyRows() {
    assertThat(formatter.format(List.of(), List.of("value"))).isEmpty();
  }

  @Test
  void shouldFormatFieldsInRequestedTableColumnOrder() {
    StarRocksRow row =
        new StarRocksRow("topic", 1, 10L, 100L, Map.of("count", 123, "value", "hello"));

    String result = formatter.format(List.of(row), List.of("value", "count", "missing"));

    assertThat(result).isEqualTo("\"topic\",1,10,100,\"hello\",123,\\N\n");
  }

  @Test
  void shouldEscapeDoubleQuotesInsideString() {
    StarRocksRow row = new StarRocksRow("topic", 0, 1L, 2L, Map.of("value", "hello \"world\""));

    assertThat(formatter.format(List.of(row), List.of("value")))
        .contains("\"hello \"\"world\"\"\"");
  }
}
