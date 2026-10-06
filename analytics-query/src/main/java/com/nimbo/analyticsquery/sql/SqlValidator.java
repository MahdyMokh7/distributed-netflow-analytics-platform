package com.nimbo.analyticsquery.sql;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.springframework.stereotype.Component;

@Component
public class SqlValidator {

  public ParsedSql validate(ParsedSql parsedSql, SqlGenerationContext context) {

    Objects.requireNonNull(parsedSql, "parsedSql must not be null");
    Objects.requireNonNull(context, "sqlGenerationContext must not be null");

    validateDataSource(parsedSql, context);
    validateStatementCount(parsedSql);

    Statement statement = parsedSql.statements().get(0);
    Select select = validateRequireSelect(statement);

    PlainSelect plainSelect = validateRequirePlainSelect(select);
    validateQueryShape(select, plainSelect);

    AstInspection inspection = inspect(statement);
    validateSelectCount(inspection);

    validateTables(inspection.tables(), context);
    validateColumns(inspection, plainSelect, context);
    validateFunctions(inspection.functions(), context.dataSource());
    validateTimeBounds(plainSelect, context);
    validateResultLimit(select);

    return parsedSql;
  }

  private void validateDataSource(ParsedSql parsedSql, SqlGenerationContext context) {

    if (parsedSql.dataSource() != context.dataSource()) {
      reject(
          SqlPolicyViolation.DATASOURCE_MISMATCH,
          "SQL datasource does not match the selected datasource");
    }
  }

  private void validateStatementCount(ParsedSql parsedSql) {
    if (parsedSql.statements().size() != 1) {
      reject(SqlPolicyViolation.STATEMENT_COUNT, "SQL must contain exactly one statement");
    }
  }

  private Select validateRequireSelect(Statement statement) {

    if (!(statement instanceof Select)) {
      reject(SqlPolicyViolation.READ_ONLY_REQUIRED, "SQL must be a read-only SELECT statement");
    }

    return (Select) statement;
  }

  private PlainSelect validateRequirePlainSelect(Select select) {

    if (!(select instanceof PlainSelect)) {
      reject(
          SqlPolicyViolation.QUERY_SHAPE_UNSUPPORTED,
          "Only a single plain SELECT query is supported in V1");
    }

    return (PlainSelect) select;
  }

  private void validateQueryShape(Select select, PlainSelect plainSelect) {

    if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
      reject(
          SqlPolicyViolation.QUERY_SHAPE_UNSUPPORTED,
          "Common table expressions are not supported in V1");
    }

    if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
      reject(
          SqlPolicyViolation.JOIN_NOT_ALLOWED, "Joins are not approved for the current V1 context");
    }

    if (!(plainSelect.getFromItem() instanceof Table)
        || plainSelect.getIntoTables() != null && !plainSelect.getIntoTables().isEmpty()
        || plainSelect.getIntoTempTable() != null
        || plainSelect.getMySqlSelectIntoClause() != null
        || select.getForUpdate() != null) {

      reject(
          SqlPolicyViolation.QUERY_SHAPE_UNSUPPORTED,
          "SELECT query shape is not supported by the V1 policy");
    }
  }

  private void validateSelectCount(AstInspection inspection) {
    if (inspection.selectCount() != 1) {
      reject(SqlPolicyViolation.QUERY_SHAPE_UNSUPPORTED, "Nested queries are not supported in V1");
    }
  }

  private AstInspection inspect(Statement statement) {

    AstInspector inspector = new AstInspector();
    Set<String> tables =
        inspector.getTables(statement).stream()
            .map(SqlValidator::canonical)
            .collect(Collectors.toUnmodifiableSet());

    return new AstInspection(
        tables,
        Set.copyOf(inspector.columns),
        Set.copyOf(inspector.functions),
        inspector.selectCount);
  }

  private void validateTables(Set<String> referencedTables, SqlGenerationContext context) {

    if (referencedTables.isEmpty()) {
      reject(SqlPolicyViolation.TABLE_REQUIRED, "SQL must reference an approved analytics table");
    }

    Set<String> allowedTables =
        context.tables().stream()
            .map(SqlGenerationContext.Table::name)
            .map(SqlValidator::canonical)
            .collect(Collectors.toUnmodifiableSet());

    if (!allowedTables.containsAll(referencedTables)) {
      reject(
          SqlPolicyViolation.TABLE_NOT_ALLOWED,
          "SQL references a table outside the selected datasource context");
    }
  }

  private void validateColumns(
      AstInspection inspection, PlainSelect select, SqlGenerationContext context) {

    boolean wildcardProjection =
        select.getSelectItems().stream()
            .map(SelectItem::getExpression)
            .anyMatch(
                expression ->
                    expression instanceof AllColumns || expression instanceof AllTableColumns);

    if (wildcardProjection) {
      reject(SqlPolicyViolation.COLUMN_NOT_ALLOWED, "Wildcard projections are not allowed");
    }

    Set<String> allowedColumns =
        context.tables().stream()
            .flatMap(table -> table.columns().stream())
            .map(SemanticColumn::name)
            .map(SqlValidator::canonical)
            .collect(Collectors.toCollection(HashSet::new));

    select.getSelectItems().stream()
        .map(SelectItem::getUnquotedAliasName)
        .filter(Objects::nonNull)
        .map(SqlValidator::canonical)
        .forEach(allowedColumns::add);

    if (!allowedColumns.containsAll(inspection.columns())) {
      reject(
          SqlPolicyViolation.COLUMN_NOT_ALLOWED,
          "SQL references a column outside the approved context");
    }
  }

  private void validateFunctions(Set<String> functions, AnalysisDataSource dataSource) {

    Set<String> allowedFunctions = SqlPolicyRules.allowedFunctions(dataSource);

    if (!allowedFunctions.containsAll(functions)) {
      reject(
          SqlPolicyViolation.FUNCTION_NOT_ALLOWED,
          "SQL uses a function outside the approved V1 allowlist");
    }
  }

  private void validateTimeBounds(PlainSelect select, SqlGenerationContext context) {

    Set<String> timeColumns =
        context.tables().stream()
            .map(SqlGenerationContext.Table::timeSemantics)
            .map(SqlGenerationContext.TimeSemantics::column)
            .map(SqlValidator::canonical)
            .collect(Collectors.toUnmodifiableSet());

    TimeBounds bounds = findConjunctiveTimeBounds(select.getWhere(), timeColumns);

    if (!bounds.lower() || !bounds.upper()) {
      reject(
          SqlPolicyViolation.TIME_BOUND_REQUIRED,
          "SQL must contain lower and upper bounds on the approved time column");
    }
  }

  private TimeBounds findConjunctiveTimeBounds(Expression expression, Set<String> timeColumns) {

    if (expression == null) {
      return TimeBounds.NONE;
    }

    if (expression instanceof ParenthesedExpressionList<?> parenthesized
        && parenthesized.size() == 1) {
      return findConjunctiveTimeBounds(parenthesized.get(0), timeColumns);
    }

    if (expression instanceof AndExpression andExpression) {
      return findConjunctiveTimeBounds(andExpression.getLeftExpression(), timeColumns)
          .merge(findConjunctiveTimeBounds(andExpression.getRightExpression(), timeColumns));
    }

    if (expression instanceof Between between
        && !between.isNot()
        && isTimeColumn(between.getLeftExpression(), timeColumns)) {
      return TimeBounds.BOTH;
    }

    if (expression instanceof EqualsTo equals
        && (isTimeColumn(equals.getLeftExpression(), timeColumns)
            || isTimeColumn(equals.getRightExpression(), timeColumns))) {
      return TimeBounds.BOTH;
    }

    if (expression instanceof GreaterThan || expression instanceof GreaterThanEquals) {
      return comparisonBounds((BinaryExpression) expression, timeColumns, true);
    }

    if (expression instanceof MinorThan || expression instanceof MinorThanEquals) {
      return comparisonBounds((BinaryExpression) expression, timeColumns, false);
    }

    return TimeBounds.NONE;
  }

  private TimeBounds comparisonBounds(
      BinaryExpression expression, Set<String> timeColumns, boolean greaterThan) {

    if (isTimeColumn(expression.getLeftExpression(), timeColumns)) {
      return greaterThan ? TimeBounds.LOWER : TimeBounds.UPPER;
    }

    if (isTimeColumn(expression.getRightExpression(), timeColumns)) {
      return greaterThan ? TimeBounds.UPPER : TimeBounds.LOWER;
    }

    return TimeBounds.NONE;
  }

  private boolean isTimeColumn(Expression expression, Set<String> timeColumns) {

    return expression instanceof Column column
        && timeColumns.contains(canonical(column.getUnquotedColumnName()));
  }

  private void validateResultLimit(Select select) {

    Long limit = null;

    if (select.getLimit() != null
        && select.getLimit().getRowCount() instanceof LongValue rowCount) {
      limit = rowCount.getValue();
    } else if (select.getFetch() != null
        && select.getFetch().getExpression() instanceof LongValue rowCount) {
      limit = rowCount.getValue();
    }

    if (limit == null || limit < 1 || limit > SqlPolicyRules.maxResultRows()) {

      reject(
          SqlPolicyViolation.RESULT_LIMIT_REQUIRED,
          "SQL must use a literal result limit within the approved maximum");
    }
  }

  private static String canonical(String identifier) {
    return identifier.replace("\"", "").replace("`", "").toLowerCase(Locale.ROOT);
  }

  private static void reject(SqlPolicyViolation violation, String message) {
    throw new SqlValidationException(violation, message);
  }

  private record AstInspection(
      Set<String> tables, Set<String> columns, Set<String> functions, int selectCount) {}

  private record TimeBounds(boolean lower, boolean upper) {

    private static final TimeBounds NONE = new TimeBounds(false, false);
    private static final TimeBounds LOWER = new TimeBounds(true, false);
    private static final TimeBounds UPPER = new TimeBounds(false, true);
    private static final TimeBounds BOTH = new TimeBounds(true, true);

    private TimeBounds merge(TimeBounds other) {
      return new TimeBounds(lower || other.lower, upper || other.upper);
    }
  }

  private static final class AstInspector extends TablesNamesFinder<Void> {

    private final Set<String> columns = new HashSet<>();
    private final Set<String> functions = new HashSet<>();
    private int selectCount;

    @Override
    public <S> Void visit(PlainSelect plainSelect, S context) {
      selectCount++;
      return super.visit(plainSelect, context);
    }

    @Override
    public <S> Void visit(Column column, S context) {
      columns.add(canonical(column.getUnquotedColumnName()));
      return super.visit(column, context);
    }

    @Override
    public <S> Void visit(Function function, S context) {
      functions.add(function.getName().toUpperCase(Locale.ROOT));
      return super.visit(function, context);
    }
  }
}
