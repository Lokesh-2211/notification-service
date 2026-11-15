package com.loki.notification.api;

import com.loki.notification.kafka.NotificationProducer;
import com.loki.notification.model.Notification;
import com.loki.notification.model.NotificationEvent;
import com.loki.notification.model.NotificationRequest;
import com.loki.notification.repository.NotificationRepository;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.Optional;

/**
 * The front door: other services call POST /api/notifications.
 *
 * We do NOT send the email here. We just put it on Kafka and answer right away.
 * That is why we return 202 ACCEPTED ("got it, will process it") instead of 200 OK.
 */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationRepository repository;
    private final NotificationProducer producer;

    public NotificationController(NotificationProducer producer,
                                  NotificationRepository repository) {
        this.producer = producer;
        this.repository = repository;
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

    @GetMapping("/{id}")
    public Notification getById(@PathVariable String id) {
        Optional<Notification> found = repository.findById(id);

        if (found.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Notification not found");
        }

        return found.get();
    }
}
