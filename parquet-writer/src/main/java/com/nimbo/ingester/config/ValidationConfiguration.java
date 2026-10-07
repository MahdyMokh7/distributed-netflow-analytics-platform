package com.nimbo.ingester.config;

import com.nimbo.common.validation.NetflowValidationService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ValidationConfiguration {

  @Bean
  public NetflowValidationService netflowValidationService() {
    return new NetflowValidationService();
  }
}
