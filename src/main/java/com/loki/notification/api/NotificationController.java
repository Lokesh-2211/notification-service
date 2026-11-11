package com.loki.notification.api;

import com.loki.notification.kafka.NotificationProducer;
import com.loki.notification.model.NotificationEvent;
import com.loki.notification.model.NotificationRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * The front door: other services call POST /api/notifications.
 *
 * We do NOT send the email here. We just put it on Kafka and answer right away.
 * That is why we return 202 ACCEPTED ("got it, will process it") instead of 200 OK.
 */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationProducer producer;

    public NotificationController(NotificationProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> send(@Valid @RequestBody NotificationRequest request) {
        NotificationEvent event = NotificationEvent.from(request);
        producer.send(event);
        return Map.of(
                "notificationId", event.notificationId(),
                "status", "ACCEPTED"
        );
    }
}
