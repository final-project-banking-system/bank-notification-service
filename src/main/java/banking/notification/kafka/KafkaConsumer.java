package banking.notification.kafka;

import banking.notification.dto.kafka.KafkaEventWrapper;
import banking.notification.dto.kafka.TransferCompletedEvent;
import banking.notification.dto.kafka.UserCreatedEvent;
import banking.notification.dto.kafka.UserLoginEvent;
import banking.notification.model.NotificationEntity;
import banking.notification.repository.NotificationRepository;
import banking.notification.service.NotificationService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {
    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;
    private final NotificationRepository notificationRepository;

    @KafkaListener(
            topics = "${banking.kafka.topics.users}",
            groupId = "${spring.application.name}"
    )
    public void handleUserCreated(String message) {
        try {
            log.info("Получено событие Kafka: {}", message);

            KafkaEventWrapper wrapper = objectMapper.readValue(message, KafkaEventWrapper.class);

            if (!"USER_CREATED".equals(wrapper.getEventType())) {
                log.debug("Пропускаем событие с типом: {}", wrapper.getEventType());
                return;
            }

            if (wrapper.getData() == null) {
                log.error("Поле data пустое для такого типа события: {}", wrapper.getEventType());
                return;
            }

            JsonNode dataNode = wrapper.getData();
            UserCreatedEvent event = objectMapper.treeToValue(dataNode, UserCreatedEvent.class);
            if (event.getUserId() != null && event.getLogin() != null && event.getEmail() != null) {
                notificationService.sendWelcomeNotification(UUID.fromString(event.getUserId()), event.getLogin(), event.getEmail());
            }

        } catch (Exception exception) {
            log.error("Не удалось обработать событие по регистрации пользователя: {}",
                    exception.getMessage(), exception);
        }
    }

    @KafkaListener(
            topics = "${banking.kafka.topics.logins}",
            groupId = "${spring.application.name}"
    )
    public void handleUserLogin(String message) {
        try {
            log.info("Получено событие Kafka: {}", message);

            KafkaEventWrapper wrapper = objectMapper.readValue(message, KafkaEventWrapper.class);

            if (!"USER_LOGIN".equals(wrapper.getEventType())) {
                log.debug("Пропускаем событие с типом: {}", wrapper.getEventType());
                return;
            }

            if (wrapper.getData() == null) {
                log.error("Поле data пустое для такого типа события: {}", wrapper.getEventType());
                return;
            }

            JsonNode dataNode = wrapper.getData();
            UserLoginEvent event = objectMapper.treeToValue(dataNode, UserLoginEvent.class);

            if (event.getUserId() != null && event.getLogin() != null) {

                Optional<NotificationEntity> notificationEntity = notificationRepository
                        .findFirstByUserIdOrderByCreatedAtDesc(event.getUserId());
                notificationEntity.ifPresent(
                        entity -> notificationService.sendLoginNotification(event.getUserId(),
                                entity.getRecipientEmail(), event.getDeviceInfo()));

            }

        } catch (Exception exception) {
            log.error("Не удалось обработать событие по аутентификации пользователя: {}",
                    exception.getMessage(), exception);
        }
    }

    @KafkaListener(
            topics = "${banking.kafka.topics.transfers}",
            groupId = "${spring.application.name}"
    )
    public void handleTransferCompleted(String message) {
        try {
            log.info("Получено событие Kafka: {}", message);

            KafkaEventWrapper wrapper = objectMapper.readValue(message, KafkaEventWrapper.class);

            if (!"TRANSFER_COMPLETED".equals(wrapper.getEventType())) {
                log.debug("Пропускаем событие с типом: {}", wrapper.getEventType());
                return;
            }

            if (wrapper.getData() == null) {
                log.error("Поле data пустое для такого типа события: {}", wrapper.getEventType());
                return;
            }

            JsonNode dataNode = wrapper.getData();
            TransferCompletedEvent event = objectMapper.treeToValue(dataNode, TransferCompletedEvent.class);
            if (event.getUserId() != null) {
                Optional<NotificationEntity> notificationEntity = notificationRepository
                        .findFirstByUserIdOrderByCreatedAtDesc(event.getUserId());
                notificationEntity.ifPresent(entity -> notificationService.sendTransferNotification(
                        event.getUserId(),
                        entity.getRecipientEmail(),
                        event.getAmount(),
                        event.getFromAccountId(),
                        event.getToAccountId()
                ));
            }
        } catch (Exception exception) {
            log.error("Не удалось обработать событие по аутентификации пользователя: {}",
                    exception.getMessage(), exception);
        }
    }
}
