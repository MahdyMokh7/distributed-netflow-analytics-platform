package com.nimbo.restapi.repository.projection;

import java.math.BigInteger;
import java.time.Instant;

public interface ConnectionWindowProjection {

  byte[] getId();

  String getSrcIp();

  String getDstIp();

  Instant getWindowStart();

  Instant getWindowEnd();

  BigInteger getInBytes();

  BigInteger getOutBytes();

  BigInteger getInPackets();

  BigInteger getOutPackets();

  Instant getIngestedAt();
}
