package com.nimbo.restapi.dto.response;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class TopBandwidthResponseDtoTest {

  @Test
  void givenTopBandwidthResponseWhenSerializedThenWindowFieldsAreAbsent() throws Exception {
    TopBandwidthResponseDto response =
        TopBandwidthResponseDto.builder()
            .ip("83.222.96.124")
            .inboundBytes(BigInteger.valueOf(9_751_430))
            .outboundBytes(BigInteger.valueOf(8_401_767))
            .totalBytes(BigInteger.valueOf(18_153_197))
            .inboundPackets(BigInteger.valueOf(7_892))
            .outboundPackets(BigInteger.valueOf(6_413))
            .totalPackets(BigInteger.valueOf(14_305))
            .build();

    JsonNode json = new ObjectMapper().valueToTree(response);

    assertThat(json.get("ip").asText()).isEqualTo("83.222.96.124");
    assertThat(json.has("windowStart")).isFalse();
    assertThat(json.has("windowEnd")).isFalse();
    assertThat(json.has("ingestedAt")).isFalse();
  }
}
