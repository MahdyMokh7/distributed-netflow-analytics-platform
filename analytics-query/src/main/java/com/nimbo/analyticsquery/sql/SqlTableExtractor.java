package com.nimbo.analyticsquery.sql;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import net.sf.jsqlparser.util.TablesNamesFinder;

@Component
public class SqlTableExtractor {

    public Set<String> extract(ParsedSql parsedSql) {
        TablesNamesFinder<Void> finder = new TablesNamesFinder<>();

        return parsedSql.statements().stream()
                .flatMap(statement -> finder.getTables(statement).stream())
                .map(SqlTableExtractor::canonical)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String canonical(String identifier) {
        return identifier
                .replace("\"", "")
                .replace("`", "")
                .toLowerCase(Locale.ROOT);
    }
}
