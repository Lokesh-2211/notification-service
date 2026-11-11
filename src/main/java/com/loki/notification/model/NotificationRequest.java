package com.loki.notification.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * What another service sends to our API.
 *
 * Example JSON:
 * {
 *   "userId": "user-42",
 *   "channel": "EMAIL",
 *   "type": "PASSWORD_RESET",
 *   "recipient": "loki@example.com",
 *   "message": "Click here to reset your password"
 * }
 */
public record NotificationRequest(
        @NotBlank String userId,      // who the notification is for
        @NotNull Channel channel,     // EMAIL, SMS or PUSH
        @NotBlank String type,        // e.g. PASSWORD_RESET, ORDER_CONFIRMED, PROMO
        @NotBlank String recipient,   // email address, phone number or device id
        @NotBlank String message      // the text to send
) {
}
