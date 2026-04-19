package com.example.notification.config;

import com.example.notification.domain.model.NotificationMessage;
import com.example.notification.handler.KafkaErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    // ──────────────────────────────────────────────
    // Topic definitions
    // ──────────────────────────────────────────────

    @Bean
    public NewTopic notificationMainTopic() {
        return TopicBuilder.name("notification-main")
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationRetry1Topic() {
        return TopicBuilder.name("notification-main-retry-1")
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationRetry2Topic() {
        return TopicBuilder.name("notification-main-retry-2")
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationDltTopic() {
        return TopicBuilder.name("notification-main-dlt")
                .partitions(6)
                .replicas(1)
                .build();
    }

    // ──────────────────────────────────────────────
    // Consumer factory
    // ──────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, NotificationMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Consumer lag tuning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10_000);

        JsonDeserializer<NotificationMessage> valueDeserializer =
                new JsonDeserializer<>(NotificationMessage.class, false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                valueDeserializer
        );
    }

    /**
     * Main listener container factory.
     * concurrency = number of consumer threads (matches or is a divisor of partition count).
     * MANUAL_IMMEDIATE offset commit gives precise control over when offsets are committed.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationMessage>
    kafkaListenerContainerFactory(KafkaErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, NotificationMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);

        // Pause the listener automatically when consumer lag exceeds threshold
        factory.getContainerProperties().setIdleBetweenPolls(0);

        return factory;
    }

    // ──────────────────────────────────────────────
    // Producer factory (used by retry handler to publish to retry/DLT topics)
    // ──────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, NotificationMessage> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, NotificationMessage> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
