package com.loki.notification.kafka;

import com.loki.notification.model.Notification;
import com.loki.notification.model.NotificationEvent;
import com.loki.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The cooks: read notifications from Kafka and "send" them.
 * Two listeners (urgent and bulk) work at the same time,
 * so urgent notifications never wait behind bulk ones.
 */
@Component
public class NotificationWorker {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

    private final NotificationRepository repository;

    public NotificationWorker(NotificationRepository repository) {
        this.repository = repository;
    }

    // 🚀 Cook #1: only handles the urgent lane
    @KafkaListener(topics = "${app.kafka.urgent-topic}")
    public void handleUrgent(NotificationEvent event) {
        log.info("🚀 URGENT lane");
        process(event);
    }

    // 📦 Cook #2: only handles the bulk lane
    @KafkaListener(topics = "${app.kafka.bulk-topic}")
    public void handleBulk(NotificationEvent event) {
        log.info("📦 BULK lane");
        process(event);
    }

    // The actual work: shared by both cooks
    private void process(NotificationEvent event) {
        // 1. Skip duplicates
        if (repository.existsById(event.notificationId())) {
            log.info("⏭️ Skipping duplicate notification {}", event.notificationId());
            return;
        }

        // 2. "Send" it (for now, just print it)
        log.info("📨 Sending {} to {} via {}: \"{}\" (id={})",
                event.type(),
                event.recipient(),
                event.channel(),
                event.message(),
                event.notificationId());

        // 🧪 TEST ONLY: pretend the email provider is down
        if (event.recipient().contains("fail")) {
            throw new RuntimeException("Email provider is down!");
        }

        // 3. Save it in the database
        repository.save(new Notification(event, "SENT"));
        log.info("💾 Saved notification {} as SENT", event.notificationId());
    }
}