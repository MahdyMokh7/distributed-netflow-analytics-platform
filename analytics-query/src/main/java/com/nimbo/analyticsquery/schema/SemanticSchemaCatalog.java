package com.nimbo.analyticsquery.schema;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.util.Collection;
import java.util.Optional;

public interface SemanticSchemaCatalog {

  Collection<SemanticField> fields();

  Optional<SemanticField> find(String name);

  Collection<SemanticTable> tables();

  Collection<SemanticTable> tables(AnalysisDataSource dataSource);

  Optional<SemanticTable> findTable(AnalysisDataSource dataSource, String tableName);
}
