package com.nimbo.batch.config;

import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.dao.ConnectionAggregateDao;
import com.nimbo.batch.dao.IpBandwidthAggregateDao;
import com.nimbo.batch.dao.ProtocolAggregateDao;
import com.nimbo.batch.dao.TrafficAggregateDao;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class JdbiConfig {

    @Bean
    public Jdbi jdbi(DataSource dataSource) {
        return Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin());
    }

    @Bean
    public BatchStateDao batchStateDao(Jdbi jdbi) {
        return jdbi.onDemand(BatchStateDao.class);
    }

    @Bean
    public TrafficAggregateDao trafficAggregateDao(Jdbi jdbi) {
        return jdbi.onDemand(TrafficAggregateDao.class);
    }

    @Bean
    public ConnectionAggregateDao connectionAggregateDao(Jdbi jdbi) {
        return jdbi.onDemand(ConnectionAggregateDao.class);
    }

    @Bean
    public ProtocolAggregateDao protocolAggregateDao(Jdbi jdbi) {
        return jdbi.onDemand(ProtocolAggregateDao.class);
    }

    @Bean
    public IpBandwidthAggregateDao ipBandwidthAggregateDao(Jdbi jdbi) {
        return jdbi.onDemand(IpBandwidthAggregateDao.class);
    }
}