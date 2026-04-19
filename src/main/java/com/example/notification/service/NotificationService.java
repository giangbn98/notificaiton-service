package com.example.notification.service;

import com.example.notification.channel.NotificationChannel;
import com.example.notification.domain.model.NotificationEntity;
import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.domain.model.NotificationStatus;
import com.example.notification.domain.repository.NotificationRepository;
import com.example.notification.factory.NotificationChannelFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationChannelFactory channelFactory;
    private final NotificationRepository repository;
    private final MongoTemplate mongoTemplate;

    /**
     * Xử lý message từ main topic lần đầu.
     *
     * Idempotency flow:
     *  1. atomicUpsert: INSERT nếu chưa có, trả về doc hiện tại nếu đã tồn tại (1 operation atomic).
     *  2. Nếu status = SENT → notification đã gửi thành công trước khi service crash → bỏ qua.
     *  3. Nếu status = PENDING / RETRYING → tiếp tục gửi.
     *
     * Kết hợp @Indexed(unique=true) trên notificationId, MongoDB đảm bảo không bao giờ
     * có 2 document cùng notificationId dù service bị restart hay Kafka reassign partition.
     */
    public void process(NotificationMessage message) throws Exception {
        NotificationEntity entity = atomicUpsert(message);

        if (entity.getStatus() == NotificationStatus.SENT) {
            log.info("Idempotency guard: notification already sent, skipping id={}", message.getNotificationId());
            return;
        }

        dispatchToChannels(message, entity);
    }

    /**
     * Xử lý message từ retry topic — document đã tồn tại, chỉ tăng retryCount.
     */
    public void processRetry(NotificationMessage message, int retryAttempt) throws Exception {
        NotificationEntity entity = repository.findByNotificationId(message.getNotificationId())
                .orElseGet(() -> {
                    // Edge case: service crash trước khi insert lần đầu, đến đây qua retry topic
                    log.warn("Entity not found on retry, creating new. id={}", message.getNotificationId());
                    return buildNewEntity(message);
                });

        if (entity.getStatus() == NotificationStatus.SENT) {
            log.info("Idempotency guard: notification already sent on retry, skipping id={}", message.getNotificationId());
            return;
        }

        entity.setStatus(NotificationStatus.RETRYING);
        entity.setRetryCount(retryAttempt);
        repository.save(entity);

        dispatchToChannels(message, entity);
    }

    /**
     * Đánh dấu notification thất bại vĩnh viễn từ DLT consumer.
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

    /**
     * Atomic upsert bằng MongoDB findAndModify:
     *  - Nếu document CHƯA tồn tại  → insert với các field setOnInsert, trả về doc mới (PENDING).
     *  - Nếu document ĐÃ tồn tại    → không thay đổi gì, trả về doc hiện tại (giữ nguyên status).
     *
     * setOnInsert chỉ chạy khi INSERT, không chạy khi UPDATE → idempotent hoàn toàn.
     * returnNew=true → luôn trả về trạng thái doc SAU operation.
     *
     * So sánh với cách cũ (findById + save):
     *  Cũ: read → check → write  (3 bước, có race window giữa 2 instance)
     *  Mới: findAndModify upsert  (1 operation atomic ở MongoDB server)
     */
    private NotificationEntity atomicUpsert(NotificationMessage message) {
        Query query = Query.query(
                Criteria.where("notificationId").is(message.getNotificationId())
        );

        Update update = new Update()
                .setOnInsert("notificationId",   message.getNotificationId())
                .setOnInsert("userId",            message.getUserId())
                .setOnInsert("title",             message.getTitle())
                .setOnInsert("body",              message.getBody())
                .setOnInsert("type",              message.getType())
                .setOnInsert("deviceToken",       message.getDeviceToken())
                .setOnInsert("recipientEmail",    message.getRecipientEmail())
                .setOnInsert("data",              message.getData())
                .setOnInsert("imageUrl",          message.getImageUrl())
                .setOnInsert("status",            NotificationStatus.PENDING)
                .setOnInsert("retryCount",        0)
                .setOnInsert("createdAt",         Instant.now());

        FindAndModifyOptions options = FindAndModifyOptions.options()
                .upsert(true)
                .returnNew(true);  // trả về doc sau operation

        NotificationEntity entity = mongoTemplate.findAndModify(
                query, update, options, NotificationEntity.class
        );

        if (entity == null) {
            // Không xảy ra với upsert=true + returnNew=true, nhưng handle phòng thủ
            throw new IllegalStateException("atomicUpsert returned null for id=" + message.getNotificationId());
        }

        log.debug("atomicUpsert id={} status={}", entity.getNotificationId(), entity.getStatus());
        return entity;
    }

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
            updateStatus(entity, NotificationStatus.RETRYING, lastException.getMessage());
            throw lastException;
        }

        updateStatus(entity, NotificationStatus.SENT, null);
    }

    private void updateStatus(NotificationEntity entity, NotificationStatus status, String failureReason) {
        entity.setStatus(status);
        entity.setFailureReason(failureReason);
        if (status == NotificationStatus.SENT) {
            entity.setSentAt(Instant.now());
        }
        repository.save(entity);
    }

    private NotificationEntity buildNewEntity(NotificationMessage message) {
        return NotificationEntity.builder()
                .notificationId(message.getNotificationId())
                .userId(message.getUserId())
                .title(message.getTitle())
                .body(message.getBody())
                .type(message.getType())
                .deviceToken(message.getDeviceToken())
                .recipientEmail(message.getRecipientEmail())
                .data(message.getData())
                .imageUrl(message.getImageUrl())
                .status(NotificationStatus.PENDING)
                .retryCount(0)
                .build();
    }
}
