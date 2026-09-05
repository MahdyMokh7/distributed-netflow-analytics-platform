package com.nimbo.batch.writer;

import com.nimbo.batch.config.ExecutorPoolConfig;
import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.dao.ConnectionAggregateDao;
import com.nimbo.batch.dao.IpBandwidthAggregateDao;
import com.nimbo.batch.dao.ProtocolAggregateDao;
import com.nimbo.batch.dao.TrafficAggregateDao;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;

@Component
public class JdbiDaoFactory implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxPoolSize;
    private final long connectionTimeoutMs;
    private final long idleTimeoutMs;
    private final long maxLifetimeMs;
    private final String poolName;

    private transient HikariDataSource dataSource;

    public JdbiDaoFactory(PostgresConfig postgresConfig, ExecutorPoolConfig executorPoolConfig) {
        this.jdbcUrl = postgresConfig.getJdbcUrl();
        this.username = postgresConfig.getUsername();
        this.password = postgresConfig.getPassword();
        this.maxPoolSize = executorPoolConfig.getMaxPoolSize();
        this.connectionTimeoutMs = executorPoolConfig.getConnectionTimeoutMs();
        this.idleTimeoutMs = executorPoolConfig.getIdleTimeoutMs();
        this.maxLifetimeMs = executorPoolConfig.getMaxLifetimeMs();
        this.poolName = executorPoolConfig.getPoolName();
    }

    private synchronized HikariDataSource getDataSource() {
        if (dataSource == null) {
            dataSource = new HikariDataSource(createHikariConfig());
        }
        return dataSource;
    }

    HikariConfig createHikariConfig() {
        PGSimpleDataSource postgresDataSource = new PGSimpleDataSource();
        postgresDataSource.setUrl(jdbcUrl);
        postgresDataSource.setUser(username);
        postgresDataSource.setPassword(password);

        HikariConfig config = new HikariConfig();
        config.setDataSource(postgresDataSource);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(connectionTimeoutMs);
        config.setIdleTimeout(idleTimeoutMs);
        config.setMaxLifetime(maxLifetimeMs);
        config.setPoolName(poolName);
        return config;
    }

    public TrafficAggregateDao createTrafficDao() {
        return createJdbi().onDemand(TrafficAggregateDao.class);
    }

    public ConnectionAggregateDao createConnectionDao() {
        return createJdbi().onDemand(ConnectionAggregateDao.class);
    }

    public ProtocolAggregateDao createProtocolDao() {
        return createJdbi().onDemand(ProtocolAggregateDao.class);
    }

    public IpBandwidthAggregateDao createIpBandwidthDao() {
        return createJdbi().onDemand(IpBandwidthAggregateDao.class);
    }

    private Jdbi createJdbi() {
        return Jdbi.create(getDataSource()).installPlugin(new SqlObjectPlugin());
    }
}
