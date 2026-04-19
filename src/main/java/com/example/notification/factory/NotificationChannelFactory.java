package com.example.notification.factory;

import com.example.notification.channel.NotificationChannel;
import com.example.notification.domain.model.NotificationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Factory pattern — resolves the correct channel(s) for a given NotificationType.
 * All NotificationChannel beans are auto-discovered via Spring DI.
 */
@Slf4j
@Component
public class NotificationChannelFactory {

    private final Map<NotificationType, NotificationChannel> channelMap;

    public NotificationChannelFactory(List<NotificationChannel> channels) {
        this.channelMap = channels.stream()
                .collect(Collectors.toMap(NotificationChannel::supportedType, Function.identity()));
        log.info("Registered notification channels: {}", channelMap.keySet());
    }

    /**
     * Returns the single channel for PUSH or EMAIL.
     * For BOTH, callers should use getChannelsFor().
     */
    public NotificationChannel getChannel(NotificationType type) {
        NotificationChannel channel = channelMap.get(type);
        if (channel == null) {
            throw new IllegalArgumentException("No channel registered for type: " + type);
        }
        return channel;
    }

    /**
     * Returns all applicable channels for the given type.
     * BOTH → [PUSH channel, EMAIL channel].
     */
    public List<NotificationChannel> getChannelsFor(NotificationType type) {
        if (type == NotificationType.BOTH) {
            return List.of(
                    getChannel(NotificationType.PUSH),
                    getChannel(NotificationType.EMAIL)
            );
        }
        return List.of(getChannel(type));
    }
}
