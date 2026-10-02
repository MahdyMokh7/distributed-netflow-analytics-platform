package com.nimbo.analyticsquery.schema;

import java.util.Collection;
import java.util.Optional;

public interface SemanticSchemaCatalog {

    Collection<SemanticField> fields();

    Optional<SemanticField> find(String name);
}