package com.nimbo.analyticsquery.sql.execution;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.nimbo.analyticsquery.config.SqlExecutionProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;

@Component
@ConditionalOnProperty(
        prefix = "analytics-query.sql-execution.starrocks",
        name = "enabled",
        havingValue = "true")
public class StarRocksSqlExecutor
        extends JdbcSqlExecutor {

    public StarRocksSqlExecutor(
            @Qualifier("starRocksSqlExecutionDataSource")
            DataSource dataSource,
            ObjectMapper objectMapper,
            SqlExecutionProperties properties) {

        super(
                AnalysisDataSource.STARROCKS,
                dataSource,
                objectMapper,
                properties);
    }
}