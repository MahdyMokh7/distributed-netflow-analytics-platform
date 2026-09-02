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
    private final String driverClassName;

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
        this.driverClassName = postgresConfig.getDriverClassName();
    }

    private synchronized HikariDataSource getDataSource() {
        if (dataSource == null) {
            // Try multiple ways to load the driver, with context classloader first
            boolean loaded = false;

            // Try 1: Context classloader (most likely to see user JARs)
            try {
                System.out.println("DRIVER CLASS = " + driverClassName);
                System.out.println("CONTEXT CLASSLOADER = " + Thread.currentThread().getContextClassLoader());
                System.out.println("FACTORY CLASSLOADER = " + JdbiDaoFactory.class.getClassLoader());
                System.out.println(
                        "DRIVER RESOURCE = " +
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResource("org/postgresql/Driver.class")
                );

                Class.forName(driverClassName, true, Thread.currentThread().getContextClassLoader());
                loaded = true;
                System.out.println("[JdbiDaoFactory] Driver loaded via context classloader");
            } catch (ClassNotFoundException e) {
                // Continue
            }

            // Try 2: This class's classloader
            if (!loaded) {
                try {
                    System.out.println("DRIVER CLASS = " + driverClassName);
                    System.out.println("CONTEXT CLASSLOADER = " + Thread.currentThread().getContextClassLoader());
                    System.out.println("FACTORY CLASSLOADER = " + JdbiDaoFactory.class.getClassLoader());
                    System.out.println(
                            "DRIVER RESOURCE = " +
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResource("org/postgresql/Driver.class")
                    );
                    Class.forName(driverClassName, true, JdbiDaoFactory.class.getClassLoader());
                    loaded = true;
                    System.out.println("[JdbiDaoFactory] Driver loaded via JdbiDaoFactory classloader");
                } catch (ClassNotFoundException e) {
                    // Continue
                }
            }

            // Try 3: System classloader
            if (!loaded) {
                try {
                    System.out.println("DRIVER CLASS = " + driverClassName);
                    System.out.println("CONTEXT CLASSLOADER = " + Thread.currentThread().getContextClassLoader());
                    System.out.println("FACTORY CLASSLOADER = " + JdbiDaoFactory.class.getClassLoader());
                    System.out.println(
                            "DRIVER RESOURCE = " +
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResource("org/postgresql/Driver.class")
                    );
                    Class.forName(driverClassName, true, ClassLoader.getSystemClassLoader());
                    loaded = true;
                    System.out.println("[JdbiDaoFactory] Driver loaded via system classloader");
                } catch (ClassNotFoundException e) {
                    // Continue
                }
            }

            // Try 4: Default (uses current class's classloader)
            if (!loaded) {
                try {
                    System.out.println("DRIVER CLASS = " + driverClassName);
                    System.out.println("CONTEXT CLASSLOADER = " + Thread.currentThread().getContextClassLoader());
                    System.out.println("FACTORY CLASSLOADER = " + JdbiDaoFactory.class.getClassLoader());
                    System.out.println(
                            "DRIVER RESOURCE = " +
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResource("org/postgresql/Driver.class")
                    );
                    Class.forName(driverClassName);
                    loaded = true;
                    System.out.println("[JdbiDaoFactory] Driver loaded via default classloader");
                } catch (ClassNotFoundException e) {
                    // Continue
                }
            }

            if (!loaded) {
                throw new RuntimeException("PostgreSQL driver not found in any classloader");
            }

            HikariConfig cfg = new HikariConfig();
            cfg.setJdbcUrl(jdbcUrl);
            cfg.setUsername(username);
            cfg.setPassword(password);
//            cfg.setDriverClassName(driverClassName);
            cfg.setMaximumPoolSize(maxPoolSize);
            cfg.setConnectionTimeout(connectionTimeoutMs);
            cfg.setIdleTimeout(idleTimeoutMs);
            cfg.setMaxLifetime(maxLifetimeMs);
            cfg.setPoolName(poolName);
            dataSource = new HikariDataSource(cfg);
        }
        return dataSource;
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