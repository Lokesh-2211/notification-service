package com.loki.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates the Kafka topic automatically when the app starts
 * (if it does not exist yet).
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic urgentTopic(@Value("${app.kafka.urgent-topic}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic bulkTopic(@Value("${app.kafka.bulk-topic}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic urgentDltTopic(@Value("${app.kafka.urgent-topic}-dlt") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic bulkDltTopic(@Value("${app.kafka.bulk-topic}-dlt") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
