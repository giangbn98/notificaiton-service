package com.example.notification.channel;

import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.domain.model.NotificationType;

/**
 * Strategy interface — each channel implements its own send logic.
 */
public interface NotificationChannel {

    NotificationType supportedType();

    void send(NotificationMessage message) throws Exception;
}
