package com.loki.notification.kafka;

import com.loki.notification.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Puts notification events on the Kafka topic.
 */
@Component
public class NotificationProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationProducer.class);

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final String topic;

    public NotificationProducer(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(NotificationEvent event) {
        // Key = userId. Kafka sends messages with the same key to the same partition,
        // so one user's notifications stay in order.
        kafkaTemplate.send(topic, event.userId(), event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed to send notification {} to Kafka", event.notificationId(), error);
                    } else {
                        log.info("Sent notification {} to topic={} partition={} offset={}",
                                event.notificationId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
