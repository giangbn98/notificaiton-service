package com.example.notification.domain.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@Document(collection = "notifications")
public class NotificationEntity {

    @Id
    private String id;

    @Indexed(unique = true)
    private String notificationId;

    @Indexed
    private String userId;

    private String title;
    private String body;
    private NotificationType type;
    private NotificationStatus status;

    private String deviceToken;
    private String recipientEmail;
    private Map<String, String> data;
    private String imageUrl;

    private int retryCount;
    private String failureReason;

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;

    private Instant sentAt;
}
