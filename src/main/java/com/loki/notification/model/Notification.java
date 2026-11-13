package com.loki.notification.model;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    private String notificationId;

    private String userId;

    @Enumerated(EnumType.STRING)
    private Channel channel;

    private String type;
    private String recipient;
    private String message;
    private String status;
    private Instant createdAt;

    protected Notification() {
    }

    public Notification(NotificationEvent event, String status) {
        this.notificationId = event.notificationId();
        this.userId = event.userId();
        this.channel = event.channel();
        this.type = event.type();
        this.recipient = event.recipient();
        this.message = event.message();
        this.status = status;
        this.createdAt = event.createdAt();
    }
}