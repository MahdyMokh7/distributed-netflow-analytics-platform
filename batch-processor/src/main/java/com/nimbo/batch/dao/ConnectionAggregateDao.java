package com.nimbo.batch.dao;

import com.nimbo.batch.model.ConnectionAggregate;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

import java.util.List;

import static com.nimbo.batch.constants.SqlConstants.CONNECTION_AGGREGATES_UPSERT;

@RegisterBeanMapper(ConnectionAggregate.class)
public interface ConnectionAggregateDao {

    @Transaction
    @SqlBatch(CONNECTION_AGGREGATES_UPSERT)
    int[] upsertBatch(@BindBean List<ConnectionAggregate> aggregates);

    @SqlUpdate(CONNECTION_AGGREGATES_UPSERT)
    int upsert(@BindBean ConnectionAggregate aggregate);
}
