package com.loki.notification.model;

import java.time.Instant;
import java.util.UUID;

/**
 * What we put on Kafka.
 *
 * It is the request plus two things we add ourselves:
 *  - notificationId: a unique ID (in Step 5 we use it to stop duplicate sends)
 *  - createdAt: when we received it
 */
public record NotificationEvent(
        String notificationId,
        String userId,
        Channel channel,
        String type,
        String recipient,
        String message,
        Instant createdAt
) {

    /** Turns an incoming request into an event, giving it a new unique ID. */
    public static NotificationEvent from(NotificationRequest request) {
        return new NotificationEvent(
                UUID.randomUUID().toString(),
                request.userId(),
                request.channel(),
                request.type(),
                request.recipient(),
                request.message(),
                Instant.now()
        );
    }
}
