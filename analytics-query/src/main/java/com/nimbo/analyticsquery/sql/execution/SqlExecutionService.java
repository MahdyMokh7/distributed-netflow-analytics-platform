package com.nimbo.analyticsquery.sql.execution;

import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import com.nimbo.analyticsquery.model.PlannerResult;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.FailureReason;
import com.nimbo.analyticsquery.metrics.AnalyticsQueryMetrics.Stage;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.schema.SemanticSchemaCatalog;
import com.nimbo.analyticsquery.schema.SemanticTable;
import com.nimbo.analyticsquery.sql.ParsedSql;
import com.nimbo.analyticsquery.sql.SqlAstParser;
import com.nimbo.analyticsquery.sql.SqlGenerationContext;
import com.nimbo.analyticsquery.sql.SqlGenerationContextFactory;
import com.nimbo.analyticsquery.sql.SqlParsingException;
import com.nimbo.analyticsquery.sql.SqlTableExtractor;
import com.nimbo.analyticsquery.sql.SqlValidationException;
import com.nimbo.analyticsquery.sql.SqlValidator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SqlExecutionService {

    private final SqlAstParser parser;
    private final SqlTableExtractor tableExtractor;
    private final SemanticSchemaCatalog catalog;
    private final SqlGenerationContextFactory contextFactory;
    private final SqlValidator validator;
    private final Map<AnalysisDataSource, QueryPlanSafetyChecker> planCheckers;
    private final Map<AnalysisDataSource, SqlExecutor> executors;
    private final AnalyticsQueryMetrics metrics;

    public SqlExecutionService(
            SqlAstParser parser,
            SqlTableExtractor tableExtractor,
            SemanticSchemaCatalog catalog,
            SqlGenerationContextFactory contextFactory,
            SqlValidator validator,
            List<QueryPlanSafetyChecker> planCheckers,
            List<SqlExecutor> executors,
            AnalyticsQueryMetrics metrics) {

        this.parser = parser;
        this.tableExtractor = tableExtractor;
        this.catalog = catalog;
        this.contextFactory = contextFactory;
        this.validator = validator;
        this.planCheckers = index(planCheckers, QueryPlanSafetyChecker::dataSource, "plan checker");
        this.executors = index(executors, SqlExecutor::dataSource, "SQL executor");
        this.metrics = metrics;
    }

    public PlannerResult execute(String sql) {
        try {
            RoutedSql routed = identifyDataSource(sql);
            SqlGenerationContext context =
                    contextFactory.create(
                            new DataSourceRoute(
                                    routed.dataSource(),
                                    routed.table()));

            return execute(routed.parsedSql(), context);

        } catch (SqlParsingException exception) {
            log.warn("Submitted SQL could not be parsed");
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_PARSE);
            return rejected();

        } catch (SqlValidationException exception) {
            log.warn("Submitted SQL was rejected by policy: {}", exception.violation());
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_POLICY);
            return rejected();
        }
    }

    public PlannerResult execute(
            ParsedSql parsedSql,
            SqlGenerationContext context) {
        try {
            ParsedSql validated = metrics.observeStage(
                    Stage.SQL_VALIDATION,
                    () -> validator.validate(parsedSql, context));

            QueryPlanSafetyChecker checker =
                    requireComponent(
                            planCheckers,
                            context.dataSource(),
                            "Query plan validation is unavailable");

            SqlExecutor executor =
                    requireComponent(
                            executors,
                            context.dataSource(),
                            "SQL execution is unavailable");

            metrics.observeStage(
                    Stage.QUERY_PLAN,
                    () -> metrics.observeDependency(
                            AnalyticsQueryMetrics.dependency(context.dataSource()),
                            () -> {
                                checker.check(validated.sql());
                                return Boolean.TRUE;
                            }));

            return PlannerResult.success(metrics.observeStage(
                    Stage.SQL_EXECUTION,
                    () -> metrics.observeDependency(
                            AnalyticsQueryMetrics.dependency(context.dataSource()),
                            () -> executor.execute(validated.sql()))));

        } catch (SqlValidationException exception) {
            log.warn("SQL was rejected by policy: {}", exception.violation());
            metrics.recordFailure(Stage.SQL_VALIDATION, FailureReason.SQL_POLICY);
            return rejected();

        } catch (QueryPlanSafetyException exception) {
            metrics.recordFailure(Stage.QUERY_PLAN, FailureReason.QUERY_PLAN);
            if (exception.reason()
                    == QueryPlanSafetyException.Reason.EXPLAIN_FAILED
                    || exception.reason()
                    == QueryPlanSafetyException.Reason.EXPLAIN_TIMEOUT) {

                return PlannerResult.failure(
                        AnalysisStatus.DEPENDENCY_UNAVAILABLE,
                        "Query plan validation is temporarily unavailable");
            }

            return rejected();

        } catch (SqlExecutionException exception) {
            log.warn("SQL execution failed with kind {}", exception.kind());
            metrics.recordFailure(Stage.SQL_EXECUTION, FailureReason.SQL_EXECUTION);

            AnalysisStatus status =
                    exception.kind() == SqlExecutionException.Kind.DATASOURCE_ERROR
                            ? AnalysisStatus.DEPENDENCY_UNAVAILABLE
                            : AnalysisStatus.EXECUTION_FAILED;

            return PlannerResult.failure(status, "SQL execution failed safely");

        } catch (MissingSqlComponentException exception) {
            log.warn("SQL execution component is unavailable for {}", exception.dataSource());
            metrics.recordFailure(Stage.SQL_EXECUTION, FailureReason.COMPONENT_UNAVAILABLE);
            return PlannerResult.failure(
                    AnalysisStatus.DEPENDENCY_UNAVAILABLE,
                    exception.getMessage());
        }
    }

    private RoutedSql identifyDataSource(String sql) {
        List<RoutedSql> matches =
                java.util.Arrays.stream(AnalysisDataSource.values())
                        .map(dataSource -> candidate(sql, dataSource))
                        .flatMap(Optional::stream)
                        .toList();

        if (matches.size() != 1) {
            throw new SqlValidationException(
                    com.nimbo.analyticsquery.sql.SqlPolicyViolation.DATASOURCE_MISMATCH,
                    "SQL must reference exactly one approved datasource table");
        }

        return matches.get(0);
    }

    private Optional<RoutedSql> candidate(
            String sql,
            AnalysisDataSource dataSource) {

        ParsedSql parsedSql;

        try {
            parsedSql = parser.parse(sql, dataSource);
        } catch (SqlParsingException exception) {
            return Optional.empty();
        }

        Set<String> tables = tableExtractor.extract(parsedSql);

        if (tables.size() != 1) {
            return Optional.empty();
        }

        String referencedTable = tables.iterator().next();

        return catalog.tables(dataSource).stream()
                .filter(table -> canonical(table.name()).equals(referencedTable))
                .findFirst()
                .map(table -> new RoutedSql(dataSource, table, parsedSql));
    }

    private PlannerResult rejected() {
        return PlannerResult.failure(
                AnalysisStatus.QUERY_REJECTED,
                "Submitted SQL did not satisfy the query safety policy");
    }

    private static <T> Map<AnalysisDataSource, T> index(
            List<T> components,
            java.util.function.Function<T, AnalysisDataSource> dataSource,
            String componentName) {

        Map<AnalysisDataSource, T> indexed =
                new EnumMap<>(AnalysisDataSource.class);

        for (T component : components) {
            AnalysisDataSource key = dataSource.apply(component);

            if (indexed.putIfAbsent(key, component) != null) {
                throw new IllegalStateException(
                        "Duplicate " + componentName + " for " + key);
            }
        }

        return Map.copyOf(indexed);
    }

    private static <T> T requireComponent(
            Map<AnalysisDataSource, T> components,
            AnalysisDataSource dataSource,
            String message) {

        T component = components.get(dataSource);

        if (component == null) {
            throw new MissingSqlComponentException(dataSource, message);
        }

        return component;
    }

    private static String canonical(String identifier) {
        return identifier
                .replace("\"", "")
                .replace("`", "")
                .toLowerCase(Locale.ROOT);
    }

    private record RoutedSql(
            AnalysisDataSource dataSource,
            SemanticTable table,
            ParsedSql parsedSql) {}

    private static final class MissingSqlComponentException
            extends RuntimeException {

        private final AnalysisDataSource dataSource;

        private MissingSqlComponentException(
                AnalysisDataSource dataSource,
                String message) {

            super(message);
            this.dataSource = dataSource;
        }

        private AnalysisDataSource dataSource() {
            return dataSource;
        }
    }
}
