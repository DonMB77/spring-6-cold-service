package com.drifter.spring_6_cold_service.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    public static final String DRINK_REQUEST_COLD_TOPIC = "drink.request.cold";
    public static final String DRINK_PREPARED_TOPIC = "drink.prepared";
}
