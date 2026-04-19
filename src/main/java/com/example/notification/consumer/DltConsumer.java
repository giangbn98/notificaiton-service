package com.example.notification.consumer;

import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Topic consumer — records permanently failed notifications in MongoDB.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DltConsumer {

    private final NotificationService notificationService;

    @KafkaListener(
            topics = "${kafka.topics.notification-dlt:notification-main-dlt}",
            groupId = "${spring.kafka.consumer.group-id}-dlt",
            concurrency = "1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlt(
            ConsumerRecord<String, NotificationMessage> record,
            Acknowledgment acknowledgment) {

        NotificationMessage message = record.value();
        String reason = extractFailureReason(record);

        log.error("DLT — notification permanently failed id={} userId={} reason={}",
                message.getNotificationId(), message.getUserId(), reason);

        notificationService.markFailed(message, reason);
        acknowledgment.acknowledge();
    }

    private String extractFailureReason(ConsumerRecord<String, NotificationMessage> record) {
        var header = record.headers().lastHeader("kafka_dlt-exception-message");
        if (header != null) {
            return new String(header.value());
        }
        return "Unknown — exhausted all retry attempts";
    }
}
