package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.util.Collection;

public interface SemanticCatalogSource {

  AnalysisDataSource dataSource();

  Collection<SemanticTable> tables();
}
