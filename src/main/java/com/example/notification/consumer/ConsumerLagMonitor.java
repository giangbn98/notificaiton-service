package com.example.notification.consumer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Monitors consumer lag and exposes it as a Prometheus gauge.
 *
 * @SchedulerLock đảm bảo khi scale nhiều instance, chỉ 1 instance
 * thực hiện check lag tại một thời điểm — tránh spam AdminClient calls
 * và duplicate metric registration.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerLagMonitor {

    private final AdminClient adminClient;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final MeterRegistry meterRegistry;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.topics.notification-main:notification-main}")
    private String mainTopic;

    @Value("${kafka.consumer.lag-threshold:10000}")
    private long lagThreshold;

    private final AtomicLong totalLag = new AtomicLong(0);

    /**
     * lockAtMostFor: lock tự động expire sau 30s phòng trường hợp instance crash giữa chừng.
     * lockAtLeastFor: giữ lock tối thiểu 10s, tránh instance khác nhảy vào ngay sau khi job xong.
     */
    @Scheduled(fixedDelayString = "${kafka.consumer.lag-check-interval-ms:15000}")
    @SchedulerLock(
            name = "consumerLagMonitor",
            lockAtMostFor = "${shedlock.lag-monitor.lock-at-most-for:PT30S}",
            lockAtLeastFor = "${shedlock.lag-monitor.lock-at-least-for:PT10S}"
    )
    public void checkConsumerLag() {
        try {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(consumerGroupId);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    result.partitionsToOffsetAndMetadata().get();

            Set<TopicPartition> topicPartitions = committedOffsets.keySet().stream()
                    .filter(tp -> mainTopic.equals(tp.topic()))
                    .collect(Collectors.toSet());

            long lag = computeTotalLag(committedOffsets, topicPartitions);
            totalLag.set(lag);

            Gauge.builder("kafka.consumer.lag", totalLag, AtomicLong::get)
                    .tag("group", consumerGroupId)
                    .tag("topic", mainTopic)
                    .register(meterRegistry);

            if (lag > lagThreshold) {
                log.warn("Consumer lag={} exceeds threshold={} — consider scaling consumers", lag, lagThreshold);
                restartStoppedContainers();
            }

            log.debug("Consumer lag check: group={} lag={}", consumerGroupId, lag);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to fetch consumer lag", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Lấy toàn bộ end offsets trong 1 lần gọi AdminClient (thay vì N+1 calls cũ).
     */
    private long computeTotalLag(Map<TopicPartition, OffsetAndMetadata> committedOffsets,
                                  Set<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        if (partitions.isEmpty()) return 0L;

        Map<TopicPartition, OffsetSpec> offsetSpecMap = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

        Map<TopicPartition, Long> endOffsets = adminClient.listOffsets(offsetSpecMap)
                .all().get()
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

        return partitions.stream()
                .mapToLong(tp -> {
                    long committed = committedOffsets.getOrDefault(tp, new OffsetAndMetadata(0)).offset();
                    long end = endOffsets.getOrDefault(tp, 0L);
                    return Math.max(0, end - committed);
                })
                .sum();
    }

    private void restartStoppedContainers() {
        for (MessageListenerContainer container : listenerRegistry.getAllListenerContainers()) {
            if (!container.isRunning()) {
                log.warn("Listener container {} is stopped — restarting", container.getListenerId());
                container.start();
            }
        }
    }
}
