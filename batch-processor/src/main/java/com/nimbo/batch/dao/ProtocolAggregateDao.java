package com.nimbo.batch.dao;

import com.nimbo.batch.model.ProtocolAggregate;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import java.util.List;

import static com.nimbo.batch.constants.SqlConstants.PROTOCOL_USAGE_AGGREGATES_UPSERT;

@RegisterBeanMapper(ProtocolAggregate.class)
public interface ProtocolAggregateDao {

    @Transaction
    @SqlBatch(PROTOCOL_USAGE_AGGREGATES_UPSERT)
    int[] upsertBatch(@BindBean List<ProtocolAggregate> aggregates);

    @SqlUpdate(PROTOCOL_USAGE_AGGREGATES_UPSERT)
    int upsert(@BindBean ProtocolAggregate aggregate);
}
