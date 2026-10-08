package com.nimbo.analyticsquery.intent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.nimbo.analyticsquery.intent.model.AnalysisIntent;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import org.junit.jupiter.api.Test;

class AnalysisIntentJsonContractTest {

  private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

  @Test
  void givenExactSourceIpContractWhenDeserializedThenRoleAndValuesArePreserved() throws Exception {
    // Given: the exact nested structure documented for an average inbound-bytes request
    String json =
        """
        {
          "status": "SUCCESS",
          "queryRequirements": {
            "dimensions": ["SOURCE_IP"],
            "metrics": [{"field": "IN_BYTES", "aggregation": "AVG"}],
            "filters": [],
            "ipRequirements": [{
              "role": "SOURCE",
              "matchType": "EXACT",
              "values": ["223.191.114.202"]
            }],
            "trafficDirection": "INBOUND",
            "timeRange": {
              "start": "2026-10-06T13:00:00Z",
              "end": "2026-10-06T15:00:00Z"
            },
            "groupBy": [],
            "granularity": null,
            "sort": [],
            "limit": null,
            "distinct": false
          },
          "clarification": {
            "required": false,
            "missingFields": [],
            "question": null
          }
        }
        """;

    // When: strict JSON deserialization is applied
    AnalysisIntent intent = strictReader().readValue(json);

    // Then: the IP requirement matches the Java contract exactly
    QueryRequirements.IpRequirement ip = intent.queryRequirements().ipRequirements().get(0);
    assertThat(ip.role()).isEqualTo(QueryRequirements.IpRole.SOURCE);
    assertThat(ip.matchType()).isEqualTo(QueryRequirements.IpMatchType.EXACT);
    assertThat(ip.values()).containsExactly("223.191.114.202");
  }

  @Test
  void givenPreviouslyObservedIpShapeWhenDeserializedThenItIsRejected() {
    // Given: the invalid shape previously returned by the LLM
    String json =
        """
        {
          "status": "SUCCESS",
          "queryRequirements": {
            "dimensions": ["SOURCE_IP"],
            "metrics": [{"field": "IN_BYTES", "aggregation": "AVG"}],
            "filters": [],
            "ipRequirements": [{
              "field": "SOURCE_IP",
              "matchType": "EXACT",
              "value": "223.191.114.202"
            }],
            "trafficDirection": "INBOUND",
            "timeRange": {
              "start": "2026-10-06T13:00:00Z",
              "end": "2026-10-06T15:00:00Z"
            },
            "groupBy": [],
            "granularity": null,
            "sort": [],
            "limit": null,
            "distinct": false
          },
          "clarification": {
            "required": false,
            "missingFields": [],
            "question": null
          }
        }
        """;

    // When: strict JSON deserialization is applied
    var assertion = assertThatThrownBy(() -> strictReader().readValue(json));

    // Then: the undocumented field/value structure cannot enter planning
    assertion.isInstanceOf(UnrecognizedPropertyException.class);
  }

  private com.fasterxml.jackson.databind.ObjectReader strictReader() {
    return objectMapper
        .readerFor(AnalysisIntent.class)
        .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  }
}
