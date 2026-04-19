package com.example.notification.service;

import com.example.notification.channel.NotificationChannel;
import com.example.notification.domain.model.NotificationEntity;
import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.domain.model.NotificationStatus;
import com.example.notification.domain.model.NotificationType;
import com.example.notification.domain.repository.NotificationRepository;
import com.example.notification.factory.NotificationChannelFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationChannelFactory channelFactory;
    private final NotificationRepository repository;

    /**
     * Main processing entry point called by the primary consumer.
     * Persists to MongoDB first (PENDING), then dispatches to channel(s).
     */
    public void process(NotificationMessage message) throws Exception {
        NotificationEntity entity = upsertEntity(message, NotificationStatus.PENDING, 0);

        dispatchToChannels(message, entity);
    }

    /**
     * Called by retry consumers — increments retryCount in MongoDB.
     */
    public void processRetry(NotificationMessage message, int retryAttempt) throws Exception {
        NotificationEntity entity = upsertEntity(message, NotificationStatus.RETRYING, retryAttempt);

        dispatchToChannels(message, entity);
    }

    /**
     * Called by DLT consumer — marks the notification as permanently failed.
     */
    public void markFailed(NotificationMessage message, String reason) {
        repository.findByNotificationId(message.getNotificationId()).ifPresent(entity -> {
            entity.setStatus(NotificationStatus.FAILED);
            entity.setFailureReason(reason);
            repository.save(entity);
            log.warn("Notification permanently failed id={} reason={}", message.getNotificationId(), reason);
        });
    }

    // ──────────────────────────────────────────────
    // Private helpers
    // ──────────────────────────────────────────────

    private void dispatchToChannels(NotificationMessage message, NotificationEntity entity) throws Exception {
        List<NotificationChannel> channels = channelFactory.getChannelsFor(message.getType());

        Exception lastException = null;
        for (NotificationChannel channel : channels) {
            try {
                channel.send(message);
            } catch (Exception e) {
                log.error("Channel {} failed for id={}", channel.supportedType(), message.getNotificationId(), e);
                lastException = e;
            }
        }

        if (lastException != null) {
            updateEntityStatus(entity, NotificationStatus.RETRYING, lastException.getMessage());
            throw lastException;
        }

        updateEntityStatus(entity, NotificationStatus.SENT, null);
    }

    private NotificationEntity upsertEntity(NotificationMessage message, NotificationStatus status, int retryCount) {
        Optional<NotificationEntity> existing = repository.findByNotificationId(message.getNotificationId());

        NotificationEntity entity = existing.orElseGet(() ->
                NotificationEntity.builder()
                        .notificationId(message.getNotificationId())
                        .userId(message.getUserId())
                        .title(message.getTitle())
                        .body(message.getBody())
                        .type(message.getType())
                        .deviceToken(message.getDeviceToken())
                        .recipientEmail(message.getRecipientEmail())
                        .data(message.getData())
                        .imageUrl(message.getImageUrl())
                        .build()
        );

        entity.setStatus(status);
        entity.setRetryCount(retryCount);
        return repository.save(entity);
    }

    private void updateEntityStatus(NotificationEntity entity, NotificationStatus status, String failureReason) {
        entity.setStatus(status);
        entity.setFailureReason(failureReason);
        if (status == NotificationStatus.SENT) {
            entity.setSentAt(Instant.now());
        }
        repository.save(entity);
    }
}
