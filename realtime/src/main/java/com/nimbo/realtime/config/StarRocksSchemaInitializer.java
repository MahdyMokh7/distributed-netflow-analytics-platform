package com.nimbo.realtime.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class StarRocksSchemaInitializer
        implements ApplicationRunner {

    private static final String CREATE_TABLE_SCRIPT =
            "sql/netflow-realtime.sql";

    private final JdbcTemplate jdbcTemplate;

    @Override
    public void run(
            ApplicationArguments args
    ) throws IOException {

        String sql =
                loadSql();
        jdbcTemplate.execute(sql);
        log.info(
                "StarRocks realtime table verified"
        );
    }

    private String loadSql(
            ) throws IOException {

        ClassPathResource resource =
                new ClassPathResource(StarRocksSchemaInitializer.CREATE_TABLE_SCRIPT);

        return StreamUtils.copyToString(
                resource.getInputStream(),
                StandardCharsets.UTF_8
        );
    }
}