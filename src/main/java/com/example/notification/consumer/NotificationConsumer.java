package com.example.notification.consumer;

import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.handler.RetryJitterPolicy;
import com.example.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 * Main Kafka consumer for notification-main topic.
 *
 * Retry topology:
 *   notification-main
 *     └─► notification-main-retry-1  (delay ~30–45s with jitter)
 *           └─► notification-main-retry-2  (delay ~5–6min with jitter)
 *                 └─► notification-main-dlt  (permanent failure)
 *
 * @RetryableTopic configures Spring Kafka to automatically route failed
 * messages to the next retry topic, storing the retry-at timestamp in
 * message headers so the consumer can pause the partition without
 * blocking any thread.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationService notificationService;
    private final RetryJitterPolicy jitterPolicy;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(
                    delay = 30_000,
                    multiplier = 10.0,
                    maxDelay = 360_000,
                    random = true          // adds ExponentialRandom jitter on top of base delay
            ),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "false",
            kafkaTemplate = "kafkaTemplate"
    )
    @KafkaListener(
            topics = "${kafka.topics.notification-main:notification-main}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${kafka.consumer.concurrency:3}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload NotificationMessage message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received notification id={} topic={} partition={} offset={}",
                message.getNotificationId(), topic, partition, offset);

        try {
            notificationService.process(message);
            acknowledgment.acknowledge();
            log.info("Processed notification id={}", message.getNotificationId());
        } catch (Exception e) {
            log.warn("Failed to process notification id={} — will retry. reason={}",
                    message.getNotificationId(), e.getMessage());
            // Re-throwing triggers @RetryableTopic routing to retry-1 then retry-2 then DLT
            throw new RuntimeException("Notification processing failed: " + e.getMessage(), e);
        }
    }

    /**
     * Retry-1 consumer — listens on the first retry topic.
     * Applies additional jitter sleep before delegating to the service.
     * The @RetryableTopic on the main consumer already handles routing here;
     * this listener adds an explicit jitter pause before processing.
     */
    @KafkaListener(
            topics = "${kafka.topics.notification-retry-1:notification-main-retry-1}",
            groupId = "${spring.kafka.consumer.group-id}-retry",
            concurrency = "1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRetry1(
            ConsumerRecord<String, NotificationMessage> record,
            Acknowledgment acknowledgment) throws InterruptedException {

        NotificationMessage message = record.value();
        int retryAttempt = extractRetryCount(record.headers());

        log.info("Retry-1 attempt={} id={}", retryAttempt, message.getNotificationId());

        long delay = jitterPolicy.computeRetry1DelayMs();
        applyJitterDelay(delay, message.getNotificationId());

        try {
            notificationService.processRetry(message, retryAttempt);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.warn("Retry-1 failed id={} — escalating to retry-2. reason={}",
                    message.getNotificationId(), e.getMessage());
            throw new RuntimeException("Retry-1 failed: " + e.getMessage(), e);
        }
    }

    /**
     * Retry-2 consumer — last chance before DLT.
     */
    @KafkaListener(
            topics = "${kafka.topics.notification-retry-2:notification-main-retry-2}",
            groupId = "${spring.kafka.consumer.group-id}-retry",
            concurrency = "1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRetry2(
            ConsumerRecord<String, NotificationMessage> record,
            Acknowledgment acknowledgment) throws InterruptedException {

        NotificationMessage message = record.value();
        int retryAttempt = extractRetryCount(record.headers());

        log.info("Retry-2 attempt={} id={}", retryAttempt, message.getNotificationId());

        long delay = jitterPolicy.computeRetry2DelayMs();
        applyJitterDelay(delay, message.getNotificationId());

        try {
            notificationService.processRetry(message, retryAttempt);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Retry-2 failed id={} — sending to DLT. reason={}",
                    message.getNotificationId(), e.getMessage());
            throw new RuntimeException("Retry-2 failed: " + e.getMessage(), e);
        }
    }

    // ──────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────

    private void applyJitterDelay(long delayMs, String notificationId) throws InterruptedException {
        log.debug("Applying jitter delay={}ms for id={}", delayMs, notificationId);
        Thread.sleep(delayMs);
    }

    private int extractRetryCount(Headers headers) {
        var header = headers.lastHeader("kafka_backoffElapsed");
        if (header == null) return 1;
        return ByteBuffer.wrap(header.value()).getInt();
    }
}
