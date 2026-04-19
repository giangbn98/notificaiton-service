package com.example.notification.channel;

import com.example.notification.domain.model.NotificationMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * Template Method pattern — defines the skeleton of the send operation.
 * Subclasses override doSend() for channel-specific logic.
 */
@Slf4j
public abstract class AbstractNotificationChannel implements NotificationChannel {

    @Override
    public final void send(NotificationMessage message) throws Exception {
        validate(message);
        log.info("Sending [{}] notification to user={} id={}",
                supportedType(), message.getUserId(), message.getNotificationId());
        doSend(message);
        log.info("Sent [{}] notification id={}", supportedType(), message.getNotificationId());
    }

    protected abstract void validate(NotificationMessage message);

    protected abstract void doSend(NotificationMessage message) throws Exception;
}
