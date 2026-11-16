package com.loki.notification.kafka;

import com.loki.notification.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class NotificationProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationProducer.class);

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final String urgentTopic;
    private final String bulkTopic;
    private final List<String> urgentTypes;

    public NotificationProducer(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                @Value("${app.kafka.urgent-topic}") String urgentTopic,
                                @Value("${app.kafka.bulk-topic}") String bulkTopic,
                                @Value("${app.kafka.urgent-types}") List<String> urgentTypes) {
        this.kafkaTemplate = kafkaTemplate;
        this.urgentTopic = urgentTopic;
        this.bulkTopic = bulkTopic;
        this.urgentTypes = urgentTypes;
    }

    public void send(NotificationEvent event) {
        // Pick the lane
        String topic;
        if (urgentTypes.contains(event.type())) {
            topic = urgentTopic;
        } else {
            topic = bulkTopic;
        }

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