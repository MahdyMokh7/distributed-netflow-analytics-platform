package com.nimbo.analyticsquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class TimeConfiguration {

    @Bean
    public Clock utcClock() {
        return Clock.systemUTC();
    }
}