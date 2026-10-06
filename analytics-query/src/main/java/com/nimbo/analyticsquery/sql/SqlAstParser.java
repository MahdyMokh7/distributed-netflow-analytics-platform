package com.nimbo.analyticsquery.sql;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.util.Objects;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.AbstractJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;
import org.springframework.stereotype.Component;

@Component
public class SqlAstParser {

  public ParsedSql parse(GeneratedSqlCandidate candidate) {

    Objects.requireNonNull(candidate, "generatedSqlCandidate must not be null");

    return parse(candidate.sql(), candidate.dataSource());
  }

  public ParsedSql parse(String sql, AnalysisDataSource dataSource) {

    if (sql == null || sql.isBlank()) {
      throw new IllegalArgumentException("sql must not be blank");
    }

    Objects.requireNonNull(dataSource, "dataSource must not be null");

    try {
      Statements statements = parseStatements(sql, dataSource);

      return new ParsedSql(dataSource, sql, statements);

    } catch (JSQLParserException exception) {
      throw new SqlParsingException("SQL could not be parsed for " + dataSource, exception);
    }
  }

  private Statements parseStatements(String sql, AnalysisDataSource dataSource)
      throws JSQLParserException {

    return switch (dataSource) {
      case POSTGRESQL ->
          CCJSqlParserUtil.parseStatements(
              sql, parser -> parser.withDialect(AbstractJSqlParser.Dialect.POSTGRESQL));

      case STARROCKS ->
          CCJSqlParserUtil.parseStatements(
              sql, parser -> parser.withDialect(AbstractJSqlParser.Dialect.MYSQL));
    };
  }
}
