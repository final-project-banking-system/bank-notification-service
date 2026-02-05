package banking.notification.repository;

import banking.notification.model.TemplateEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface TemplateRepository extends JpaRepository<TemplateEntity, UUID> {
    Optional<TemplateEntity> findByType(String type);
}
