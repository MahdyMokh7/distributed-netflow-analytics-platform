package com.nimbo.cubemaker.writer;

import com.nimbo.cubemaker.config.ExecutorPoolConfig;
import com.nimbo.cubemaker.config.PostgresConfig;
import com.nimbo.cubemaker.dao.ConnectionAggregateDao;
import com.nimbo.cubemaker.dao.IpBandwidthAggregateDao;
import com.nimbo.cubemaker.dao.ProtocolAggregateDao;
import com.nimbo.cubemaker.dao.TrafficAggregateDao;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class JdbiDaoFactory implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxPoolSize;
    private final int minimumIdle;
    private final long connectionTimeoutMs;
    private final long idleTimeoutMs;
    private final long maxLifetimeMs;
    private final String poolName;

    public JdbiDaoFactory(PostgresConfig postgresConfig, ExecutorPoolConfig executorPoolConfig) {
        this.jdbcUrl = postgresConfig.getJdbcUrl();
        this.username = postgresConfig.getUsername();
        this.password = postgresConfig.getPassword();
        this.maxPoolSize = executorPoolConfig.getMaxPoolSize();
        this.minimumIdle = executorPoolConfig.getMinimumIdle();
        this.connectionTimeoutMs = executorPoolConfig.getConnectionTimeoutMs();
        this.idleTimeoutMs = executorPoolConfig.getIdleTimeoutMs();
        this.maxLifetimeMs = executorPoolConfig.getMaxLifetimeMs();
        this.poolName = executorPoolConfig.getPoolName();
    }

    private HikariDataSource getDataSource() {
        PoolKey poolKey = new PoolKey(jdbcUrl, username, password, poolName);
        return ExecutorPoolRegistry.getOrCreate(poolKey, this::createHikariConfig);
    }

    HikariConfig createHikariConfig() {
        PGSimpleDataSource postgresDataSource = new PGSimpleDataSource();
        postgresDataSource.setUrl(jdbcUrl);
        postgresDataSource.setUser(username);
        postgresDataSource.setPassword(password);
        postgresDataSource.setReWriteBatchedInserts(true);

        HikariConfig config = new HikariConfig();
        config.setDataSource(postgresDataSource);
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(minimumIdle);
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

    public Connection openConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private Jdbi createJdbi() {
        return Jdbi.create(getDataSource()).installPlugin(new SqlObjectPlugin());
    }

    static void closeExecutorPools() {
        ExecutorPoolRegistry.closeAll();
    }

    private record PoolKey(String jdbcUrl, String username, String password, String poolName) {
        private PoolKey {
            Objects.requireNonNull(jdbcUrl);
            Objects.requireNonNull(username);
            Objects.requireNonNull(password);
            Objects.requireNonNull(poolName);
        }
    }

    private static final class ExecutorPoolRegistry {

        private static final ConcurrentMap<PoolKey, HikariDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

        static {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    ExecutorPoolRegistry::closeAll,
                    "postgres-executor-pool-shutdown"));
        }

        private ExecutorPoolRegistry() {
        }

        private static HikariDataSource getOrCreate(PoolKey poolKey, java.util.function.Supplier<HikariConfig> config) {
            return DATA_SOURCES.computeIfAbsent(poolKey, ignored -> new HikariDataSource(config.get()));
        }

        private static void closeAll() {
            DATA_SOURCES.forEach((ignored, dataSource) -> dataSource.close());
            DATA_SOURCES.clear();
        }
    }
}
