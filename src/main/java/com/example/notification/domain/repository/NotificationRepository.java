package com.example.notification.domain.repository;

import com.example.notification.domain.model.NotificationEntity;
import com.example.notification.domain.model.NotificationStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface NotificationRepository extends MongoRepository<NotificationEntity, String> {

    Optional<NotificationEntity> findByNotificationId(String notificationId);

    Page<NotificationEntity> findByUserIdOrderByCreatedAtDesc(String userId, Pageable pageable);

    List<NotificationEntity> findByStatus(NotificationStatus status);
}
