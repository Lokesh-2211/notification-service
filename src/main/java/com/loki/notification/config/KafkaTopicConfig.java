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
    public NewTopic notificationsTopic(@Value("${app.kafka.topic}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3)   // 3 "lanes" inside the topic, so up to 3 workers can read in parallel
                .replicas(1)     // only 1 copy, because we run a single Kafka broker locally
                .build();
    }
}
