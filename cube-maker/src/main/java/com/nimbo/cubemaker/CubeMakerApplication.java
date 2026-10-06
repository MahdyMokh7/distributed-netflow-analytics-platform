package com.nimbo.cubemaker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CubeMakerApplication {

  public static void main(String[] args) {
    SpringApplication.run(CubeMakerApplication.class, args);
  }
}
