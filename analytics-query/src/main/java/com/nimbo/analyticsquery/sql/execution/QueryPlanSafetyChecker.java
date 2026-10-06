package com.nimbo.analyticsquery.sql.execution;

import com.nimbo.analyticsquery.model.AnalysisDataSource;

public interface QueryPlanSafetyChecker {

  AnalysisDataSource dataSource();

  void check(String sql);
}
