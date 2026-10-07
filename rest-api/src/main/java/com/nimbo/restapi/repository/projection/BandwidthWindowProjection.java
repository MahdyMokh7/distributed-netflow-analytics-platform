package com.nimbo.restapi.repository.projection;

import java.math.BigInteger;
import java.time.Instant;

public interface BandwidthWindowProjection {

  String getIp();

  Instant getWindowStart();

  Instant getWindowEnd();

  BigInteger getInboundBytes();

  BigInteger getOutboundBytes();

  BigInteger getInboundPackets();

  BigInteger getOutboundPackets();

  Instant getIngestedAt();
}
