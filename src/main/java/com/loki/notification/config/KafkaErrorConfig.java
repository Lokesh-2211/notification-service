package com.loki.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * What the worker should do when handling a notification fails:
 * retry 3 times (waiting 1s, 2s, 4s), then move it to the dead-letter topic.
 */
@Configuration
public class KafkaErrorConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {

        // 💀 After the last retry: put the message in "<topic>-dlt"
        DeadLetterPublishingRecoverer sendToDlt = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // ⏳ Retry 3 times, waiting longer each time: 1s → 2s → 4s
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000);   // first wait: 1000 ms = 1 second
        backOff.setMultiplier(2.0);         // each wait is 2× the previous one

        return new DefaultErrorHandler(sendToDlt, backOff);
    }
}