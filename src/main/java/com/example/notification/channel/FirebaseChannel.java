package com.example.notification.channel;

import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.domain.model.NotificationType;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class FirebaseChannel extends AbstractNotificationChannel {

    private final FirebaseMessaging firebaseMessaging;

    @Override
    public NotificationType supportedType() {
        return NotificationType.PUSH;
    }

    @Override
    protected void validate(NotificationMessage message) {
        if (!StringUtils.hasText(message.getDeviceToken())) {
            throw new IllegalArgumentException("Device token is required for PUSH notification id=" + message.getNotificationId());
        }
    }

    @Override
    protected void doSend(NotificationMessage message) throws Exception {
        Message.Builder builder = Message.builder()
                .setToken(message.getDeviceToken())
                .setNotification(Notification.builder()
                        .setTitle(message.getTitle())
                        .setBody(message.getBody())
                        .setImage(message.getImageUrl())
                        .build());

        Map<String, String> data = message.getData();
        if (data != null && !data.isEmpty()) {
            builder.putAllData(data);
        }

        if (StringUtils.hasText(message.getClickAction())) {
            builder.putData("click_action", message.getClickAction());
        }

        String messageId = firebaseMessaging.send(builder.build());
        log.debug("Firebase message sent id={} firebaseId={}", message.getNotificationId(), messageId);
    }
}
