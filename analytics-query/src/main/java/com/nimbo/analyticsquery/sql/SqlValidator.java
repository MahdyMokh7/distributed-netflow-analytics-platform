package com.nimbo.analyticsquery.sql;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

@Component
public class SqlValidator {

    public ParsedSql validate(
            ParsedSql parsedSql,
            SqlGenerationContext context) {

        Objects.requireNonNull(parsedSql, "parsedSql must not be null");
        Objects.requireNonNull(context, "sqlGenerationContext must not be null");

        validateDataSource(parsedSql, context);

        if (parsedSql.statements().size() != 1) {
            throw new SqlValidationException("Generated SQL must contain exactly one statement");
        }

        Statement statement = parsedSql.statements().get(0);

        if (!(statement instanceof Select)) {
            throw new SqlValidationException("Generated SQL must be a SELECT statement");
        }

        validateTables(statement, context);

        return parsedSql;
    }

    private void validateDataSource(
            ParsedSql parsedSql,
            SqlGenerationContext context) {

        if (parsedSql.dataSource() != context.dataSource()) {
            throw new SqlValidationException("Generated SQL datasource does not match the selected datasource");
        }
    }

    private void validateTables(
            Statement statement,
            SqlGenerationContext context) {

        Set<String> referencedTables = new TablesNamesFinder<Void>().getTables(statement);

        if (referencedTables.isEmpty()) {
            throw new SqlValidationException("Generated SQL must reference an approved table");
        }

        Set<String> allowedTables = context.tables().stream()
                .map(SqlGenerationContext.Table::name)
                .collect(Collectors.toUnmodifiableSet());

        Set<String> unknownTables = referencedTables.stream()
                .filter(table -> !allowedTables.contains(table))
                .collect(Collectors.toCollection(TreeSet::new));

        if (!unknownTables.isEmpty()) {
            throw new SqlValidationException(
                    "Generated SQL references unknown or unapproved tables: "
                            + String.join(", ", unknownTables));
        }
    }
}
