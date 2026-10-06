package com.nimbo.cubemaker.dao;

import static com.nimbo.cubemaker.constants.SqlConstants.IP_BANDWIDTH_AGGREGATES_UPSERT;

import com.nimbo.cubemaker.model.TopIpAggregate;
import java.util.List;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterBeanMapper(TopIpAggregate.class)
public interface IpBandwidthAggregateDao {

  @Transaction
  @SqlBatch(IP_BANDWIDTH_AGGREGATES_UPSERT)
  int[] upsertBatch(@BindBean List<TopIpAggregate> aggregates);

  @SqlUpdate(IP_BANDWIDTH_AGGREGATES_UPSERT)
  int upsert(@BindBean TopIpAggregate aggregate);
}
