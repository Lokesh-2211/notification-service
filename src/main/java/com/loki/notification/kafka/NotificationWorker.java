package com.loki.notification.kafka;

import com.loki.notification.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The cook: reads notifications from Kafka and "sends" them.
 * For now, sending just means printing to the console.
 */
@Component
public class NotificationWorker {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

    @KafkaListener(topics = "${app.kafka.topic}")
    public void handle(NotificationEvent event) {
        log.info("📨 Sending {} to {} via {}: \"{}\" (id={})",
                event.type(),
                event.recipient(),
                event.channel(),
                event.message(),
                event.notificationId());
    }
}