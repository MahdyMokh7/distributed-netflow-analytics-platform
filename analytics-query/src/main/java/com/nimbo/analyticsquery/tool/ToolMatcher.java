package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class ToolMatcher {

  private final ToolRegistry registry;

  public ToolMatcher(ToolRegistry registry) {
    this.registry = registry;
  }

  public Optional<AnalyticsTool> findTool(QueryRequirements requirements) {

    List<AnalyticsTool> matches =
        registry.getTools().stream().filter(tool -> tool.supports(requirements)).toList();

    if (matches.size() > 1) {
      throw new IllegalStateException(
          "Multiple analytics tools support the same requirements: "
              + matches.stream().map(AnalyticsTool::name).sorted().toList());
    }

    return matches.stream().findFirst();
  }
}
