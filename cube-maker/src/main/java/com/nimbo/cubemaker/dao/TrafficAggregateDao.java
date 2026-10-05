package com.nimbo.cubemaker.dao;

import com.nimbo.cubemaker.model.TrafficAggregate;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import java.util.List;

import static com.nimbo.cubemaker.constants.SqlConstants.TRAFFIC_AGGREGATES_UPSERT;

@RegisterBeanMapper(TrafficAggregate.class)
public interface TrafficAggregateDao {

    @Transaction
    @SqlBatch(TRAFFIC_AGGREGATES_UPSERT)
    int[] upsertBatch(@BindBean List<TrafficAggregate> aggregates);

    @SqlUpdate(TRAFFIC_AGGREGATES_UPSERT)
    int upsert(@BindBean TrafficAggregate aggregate);
}
