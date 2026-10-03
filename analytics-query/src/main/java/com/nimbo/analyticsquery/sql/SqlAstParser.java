package com.nimbo.analyticsquery.sql;

import java.util.Objects;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.model.AnalysisDataSource;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.AbstractJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;

@Component
public class SqlAstParser {

    public ParsedSql parse(GeneratedSqlCandidate candidate) {

        Objects.requireNonNull(candidate, "generatedSqlCandidate must not be null");

        try {
            Statements statements = parseStatements(candidate.sql(), candidate.dataSource());

            return new ParsedSql(candidate.dataSource(), candidate.sql(), statements);

        } catch (JSQLParserException exception) {
            throw new SqlParsingException(
                    "Generated SQL could not be parsed for " + candidate.dataSource(), exception);
        }
    }

    private Statements parseStatements(String sql, AnalysisDataSource dataSource) throws JSQLParserException {

        return switch (dataSource) {
            case POSTGRESQL -> CCJSqlParserUtil.parseStatements(
                    sql, parser -> parser.withDialect(AbstractJSqlParser.Dialect.POSTGRESQL));

            case STARROCKS -> CCJSqlParserUtil.parseStatements(
                    sql, parser -> parser.withDialect(AbstractJSqlParser.Dialect.MYSQL));
        };
    }
}
