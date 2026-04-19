package com.example.notification.config;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.mongo.MongoLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Centralized scheduler config with ShedLock for distributed lock.
 *
 * Khi scale service lên nhiều instance, @Scheduled sẽ chạy đồng thời
 * trên tất cả instance. ShedLock dùng MongoDB làm distributed lock store
 * để đảm bảo mỗi job chỉ chạy trên đúng 1 instance tại một thời điểm.
 *
 * Flow:
 *   Instance A acquire lock → chạy job → release lock
 *   Instance B, C → không acquire được → skip
 *
 * Collection "shedLock" trong MongoDB lưu trạng thái lock theo tên job.
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "${shedlock.default-lock-at-most-for:PT30S}")
public class SchedulerConfig {

    @Value("${shedlock.collection-name:shedLock}")
    private String collectionName;

    /**
     * LockProvider dùng MongoTemplate để read/write lock document.
     * Document structure trong MongoDB:
     * {
     *   "_id": "consumerLagMonitor",
     *   "lockUntil": ISODate("2024-01-01T00:00:30Z"),
     *   "lockedAt":  ISODate("2024-01-01T00:00:00Z"),
     *   "lockedBy":  "notification-service-pod-1"
     * }
     */
    @Bean
    public LockProvider lockProvider(MongoTemplate mongoTemplate) {
        return new MongoLockProvider(mongoTemplate.getCollection(collectionName));
    }
}
