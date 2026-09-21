package com.nimbo.starrockswriter.mapper;

import com.nimbo.starrockswriter.model.StarRocksRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvFormatterTest {

    private final CsvFormatter formatter = new CsvFormatter();

    @Test
    void shouldReturnEmptyCsvForEmptyRows() {
        String result = formatter.format(List.of());

        assertThat(result).isEmpty();
    }

    @Test
    void shouldFormatSingleRow() {
        StarRocksRow row = new StarRocksRow(
                "topic",
                1,
                10L,
                100L,
                List.of("value", 123)
        );

        String result = formatter.format(List.of(row));

        assertThat(result).isEqualTo("\"topic\",1,10,100,\"value\",123\n");
    }

    @Test
    void shouldFormatMultipleRows() {
        StarRocksRow first = new StarRocksRow("a", 0, 1L, 2L, List.of("x"));
        StarRocksRow second = new StarRocksRow("b", 1, 2L, 3L, List.of("y"));

        String result = formatter.format(List.of(first, second));

        assertThat(result)
                .contains("\"a\",0,1,2,\"x\"\n")
                .contains("\"b\",1,2,3,\"y\"\n");
    }

    @Test
    void shouldWriteNullAsStarRocksNull() {

        List<Object> values = new ArrayList<>();

        values.add(null);

        StarRocksRow row = new StarRocksRow("topic", 0, 1L, 2L, values);


        String result = formatter.format(List.of(row));

        assertThat(result).contains("\\N");
    }

    @Test
    void shouldEscapeDoubleQuotesInsideString() {
        StarRocksRow row = new StarRocksRow(
                "topic",
                0,
                1L,
                2L,
                List.of("hello \"world\"")
        );

        String result = formatter.format(List.of(row));

        assertThat(result).contains("\"hello \"\"world\"\"\"");
    }

    @Test
    void shouldHandleDifferentNumberTypes() {
        StarRocksRow row = new StarRocksRow(
                "topic",
                1,
                2L,
                3L,
                List.of(10, 20L, 1.5, 2.5f)
        );

        String result = formatter.format(List.of(row));

        assertThat(result).contains("10,20,1.5,2.5");
    }
}
