package com.example.notification.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka message synced from DB notification via Golden Gate.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationMessage {

    private String notificationId;
    private String userId;
    private String title;
    private String body;
    private NotificationType type;

    /** Firebase device token — required when type is PUSH or BOTH */
    private String deviceToken;

    /** Recipient email — required when type is EMAIL or BOTH */
    private String recipientEmail;

    /** Arbitrary key-value data attached to the push notification */
    private Map<String, String> data;

    private String imageUrl;
    private String clickAction;

    /** Source operation from Golden Gate: INSERT / UPDATE / DELETE */
    private String operation;

    private Instant createdAt;
}
