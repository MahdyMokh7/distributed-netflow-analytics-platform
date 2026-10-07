package com.nimbo.restapi.repository.projection;

import java.math.BigInteger;
import java.time.Instant;

public interface ProtocolWindowProjection {

  Double getL7Protocol();

  Instant getWindowStart();

  Instant getWindowEnd();

  BigInteger getInBytes();

  BigInteger getOutBytes();

  BigInteger getInPackets();

  BigInteger getOutPackets();

  Long getConnectionCount();

  Instant getIngestedAt();
}
