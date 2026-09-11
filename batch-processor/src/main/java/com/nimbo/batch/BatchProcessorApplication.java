package com.nimbo.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BatchProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchProcessorApplication.class, args);
    }
}
