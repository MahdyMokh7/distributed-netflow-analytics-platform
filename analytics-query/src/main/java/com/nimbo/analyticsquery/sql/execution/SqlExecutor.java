package com.nimbo.analyticsquery.sql.execution;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalyticsResult;

public interface SqlExecutor {

    AnalysisDataSource dataSource();

    AnalyticsResult execute(String sql);
}