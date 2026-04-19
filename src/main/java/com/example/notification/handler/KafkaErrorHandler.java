package com.example.notification.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Common Kafka error handler — logs unhandled errors at the container level.
 * Per-message retry logic lives in NotificationConsumer via @RetryableTopic.
 */
@Slf4j
@Component
public class KafkaErrorHandler implements CommonErrorHandler {

    @Override
    public boolean handleOne(Exception thrownException,
                             ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             MessageListenerContainer container) {
        log.error("Kafka error on topic={} partition={} offset={} — message will be forwarded to retry topic",
                record.topic(), record.partition(), record.offset(), thrownException);
        return false; // let @RetryableTopic take over
    }

    @Override
    public void handleOtherException(Exception thrownException,
                                     Consumer<?, ?> consumer,
                                     MessageListenerContainer container,
                                     boolean batchListener) {
        log.error("Unrecoverable Kafka container error — container={}", container.getListenerId(), thrownException);
    }
}
