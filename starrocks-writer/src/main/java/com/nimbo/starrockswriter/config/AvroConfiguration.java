package com.nimbo.starrockswriter.config;

import org.apache.avro.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;

@Configuration
public class AvroConfiguration {

    @Bean
    public Schema netflowSchema() throws IOException {

        ClassPathResource resource =
                new ClassPathResource(
                        "avro/NetFlowV2Record.avsc"
                );

        try (InputStream inputStream =
                     resource.getInputStream()) {

            return new Schema.Parser()
                    .parse(inputStream);
        }
    }
}
