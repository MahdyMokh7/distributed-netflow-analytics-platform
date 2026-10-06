package com.nimbo.analyticsquery.tool;

import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class ToolRegistry {

  private final List<AnalyticsTool> tools;

  public ToolRegistry(List<AnalyticsTool> tools) {
    validateUniqueNames(tools);
    this.tools = List.copyOf(tools);
  }

  public List<AnalyticsTool> getTools() {
    return tools;
  }

  private void validateUniqueNames(List<AnalyticsTool> tools) {

    long uniqueCount = tools.stream().map(AnalyticsTool::name).distinct().count();

    if (uniqueCount != tools.size()) {
      throw new IllegalStateException("Duplicate analytics tool names detected");
    }
  }
}
