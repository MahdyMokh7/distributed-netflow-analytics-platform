package com.nimbo.restapi.repository.projection;

import java.math.BigInteger;
import java.time.Instant;

public interface TrafficWindowProjection {

  String getSrcIp();

  Instant getWindowStart();

  Instant getWindowEnd();

  BigInteger getInBytes();

  BigInteger getOutBytes();

  BigInteger getInPackets();

  BigInteger getOutPackets();

  BigInteger getRecordCount();

  Instant getIngestedAt();
}
