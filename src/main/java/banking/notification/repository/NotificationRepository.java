package banking.notification.repository;

import banking.notification.model.NotificationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<NotificationEntity, UUID> {
    Optional<NotificationEntity> findFirstByUserIdOrderByCreatedAtDesc(UUID userId);
}
