package com.nimbo.starrockswriter.config;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.starrockswriter.validation.SchemaAwareNetflowValidationService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonValidationConfiguration {

    @Bean
    public NetflowValidationService netflowValidationService() {
        return new SchemaAwareNetflowValidationService();
    }
}
