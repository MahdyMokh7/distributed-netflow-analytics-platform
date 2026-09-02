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

    // I removed the other 4 DAO beans because now they are all created in each executor (not in driver)
}