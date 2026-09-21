package com.nimbo.starrockswriter.mapper;

import com.nimbo.starrockswriter.model.StarRocksRow;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CsvFormatter {

    public String format(List<StarRocksRow> rows) {
        StringBuilder csv = new StringBuilder(rows.size() * 256);

        for (StarRocksRow row : rows) {
            appendRow(csv, row.values());
            csv.append('\n');
        }
        return csv.toString();
    }

    private void appendRow(StringBuilder csv, List<Object> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                csv.append(',');
            }
            appendValue(csv, values.get(i));
        }
    }

    private void appendValue(StringBuilder csv, Object value) {
        if (value == null) {
            csv.append("\\N");
            return;
        }

        if (value instanceof String stringValue) {
            appendString(csv, stringValue);
            return;
        }

        csv.append(value);
    }

    private void appendString(StringBuilder csv, String value) {
        csv.append('"');

        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> csv.append("\"\"");
                case '\n' -> csv.append("\\n");
                case '\r' -> csv.append("\\r");
                case '\\' -> csv.append("\\\\");
                default -> csv.append(c);
            }
        }
        csv.append('"');
    }
}
