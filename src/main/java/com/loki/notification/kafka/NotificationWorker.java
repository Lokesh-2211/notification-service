package com.loki.notification.kafka;

import com.loki.notification.model.Notification;
import com.loki.notification.model.NotificationEvent;
import com.loki.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationWorker {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

    private final NotificationRepository repository;

    public NotificationWorker(NotificationRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.kafka.topic}")
    public void handle(NotificationEvent event) {

        if (repository.existsById(event.notificationId())) {
            log.info("⏭️ Skipping duplicate notification {}", event.notificationId());
            return;
        }

        log.info("📨 Sending {} to {} via {}: \"{}\" (id={})",
                event.type(),
                event.recipient(),
                event.channel(),
                event.message(),
                event.notificationId());

        repository.save(new Notification(event, "SENT"));
        log.info("💾 Saved notification {} as SENT", event.notificationId());
    }
}