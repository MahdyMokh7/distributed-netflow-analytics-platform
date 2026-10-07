package com.nimbo.restapi.repository.projection;

import java.math.BigInteger;

public interface BandwidthSummaryProjection {

  String getIp();

  BigInteger getInboundBytes();

  BigInteger getOutboundBytes();

  BigInteger getInboundPackets();

  BigInteger getOutboundPackets();

  BigInteger getTotalBytes();
}
