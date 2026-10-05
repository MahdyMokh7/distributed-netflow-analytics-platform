package com.nimbo.backfill;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.repository.StarRocksBackfillRepository;
import com.nimbo.backfill.service.BackfillPlanner;
import com.nimbo.backfill.service.BackfillService;

public final class StarRocksBackfillApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksBackfillApplication.class);

    private StarRocksBackfillApplication() {
    }

    public static void main(String[] args) {
        int exitCode = run(System.getenv(), Clock.systemUTC());
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(Map<String, String> environment, Clock clock) {

        try {
            BackfillConfig config = BackfillConfig.fromEnvironment(environment, clock);
            List<DailyBackfillPlan> plans = new BackfillPlanner().createPlans(config);

            try (Connection connection =
                    DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password())) {

                StarRocksBackfillRepository repository = new StarRocksBackfillRepository(connection, config);
                repository.initializeSession();
                BackfillService service = new BackfillService(repository);
                for (DailyBackfillPlan plan : plans) {
                    service.run(plan);
                }
            }

            return 0;

        } catch (BackfillException | SQLException exception) {
            LOGGER.error("Backfill failed: {}", exception.getMessage(), exception);
            return 1;
        }
    }
}
