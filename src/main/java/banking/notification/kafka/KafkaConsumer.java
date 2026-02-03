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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {
    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;
    private final NotificationRepository notificationRepository;
    private final ConcurrentHashMap<String, KafkaEventWrapper> kafkaEvents = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${banking.kafka.topics.users}",
            groupId = "${spring.application.name}",
            containerFactory = "kafkaListenerManualCommitContainerFactory"
    )
    public void handleUserCreated(@Payload String message,
                                  ConsumerRecord<String, String> record,
                                  Acknowledgment acknowledgment) throws Exception {
        String messageId = String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
        try {
            if (kafkaEvents.containsKey(messageId)) {
                log.info("Дубликат пропущен [id={}]:",  messageId);
                acknowledgment.acknowledge();
                return;
            }

            log.info("Получено событие Kafka [id={}]: partition={}, offset={}",
                    messageId, record.partition(), record.offset());

            KafkaEventWrapper wrapper = objectMapper.readValue(message, KafkaEventWrapper.class);

            if (!"USER_CREATED".equals(wrapper.getEventType())) {
                log.debug("Пропускаем событие с типом: {}", wrapper.getEventType());
                acknowledgment.acknowledge();
                return;
            }

            if (wrapper.getData() == null) {
                log.error("Поле data пустое для USER_CREATED события");
                throw new IllegalArgumentException("Data is null for USER_CREATED event");
            }

            JsonNode dataNode = wrapper.getData();
            UserCreatedEvent event = objectMapper.treeToValue(dataNode, UserCreatedEvent.class);
            if (event.getUserId() == null || event.getLogin() == null || event.getEmail() == null) {
                log.error("Недостаточно данных: userId={}, login={}, email={}",
                        event.getUserId(), event.getLogin(), event.getEmail());
                throw new IllegalArgumentException("Missing required fields: userId or email or login");
            }

            notificationService.sendWelcomeNotification(UUID.fromString(event.getUserId()), event.getLogin(), event.getEmail());
            kafkaEvents.put(messageId, wrapper);
            acknowledgment.acknowledge();
            log.info("Обработано событие по регистрации пользователя: userId={}, messageId={}", event.getUserId(), messageId);
        } catch (Exception exception) {
            log.error("Не удалось обработать событие [id={}] по регистрации пользователя: {}",
                    messageId, exception.getMessage(), exception);
            acknowledgment.acknowledge();
            throw exception;
        }
    }

    @KafkaListener(
            topics = "${banking.kafka.topics.logins}",
            groupId = "${spring.application.name}",
            containerFactory = "kafkaListenerManualCommitContainerFactory"
    )
    public void handleUserLogin(@Payload String message,
                                ConsumerRecord<String, String> record,
                                Acknowledgment acknowledgment) throws Exception {
        String messageId = String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
        try {
            if (kafkaEvents.containsKey(messageId)) {
                log.info("Дубликат пропущен [id={}]:",  messageId);
                acknowledgment.acknowledge();
                return;
            }

            log.info("Получено событие Kafka [id={}]: partition={}, offset={}",
                    messageId, record.partition(), record.offset());

            KafkaEventWrapper wrapper = objectMapper.readValue(message, KafkaEventWrapper.class);

            if (!"USER_LOGIN".equals(wrapper.getEventType())) {
                log.debug("Пропускаем событие с типом: {}", wrapper.getEventType());
                acknowledgment.acknowledge();
                return;
            }

            if (wrapper.getData() == null) {
                log.error("Поле data пустое для USER_LOGIN события");
                throw new IllegalArgumentException("Data is null for USER_LOGIN event");
            }

            JsonNode dataNode = wrapper.getData();
            UserLoginEvent event = objectMapper.treeToValue(dataNode, UserLoginEvent.class);

            if (event.getUserId() == null || event.getLogin() == null) {
                log.error("Недостаточно данных: userId={}, login={}",
                        event.getUserId(), event.getLogin());
                throw new IllegalArgumentException("Missing required fields: userId or login");
            }

            Optional<NotificationEntity> notificationEntity = notificationRepository
                    .findFirstByUserIdOrderByCreatedAtDesc(event.getUserId());
            notificationEntity.ifPresent(
                    entity -> notificationService.sendLoginNotification(event.getUserId(),
                            entity.getRecipientEmail(), event.getDeviceInfo()));
            kafkaEvents.put(messageId, wrapper);
            acknowledgment.acknowledge();
            log.info("Обработано событие по аутентификации пользователя: userId={}, messageId={}", event.getUserId(), messageId);
        } catch (Exception exception) {
            log.error("Не удалось обработать событие [id={}] по аутентификации пользователя: {}",
                    messageId, exception.getMessage(), exception);
            acknowledgment.acknowledge();
            throw exception;
        }
    }

    @KafkaListener(
            topics = "${banking.kafka.topics.transfers}",
            groupId = "${spring.application.name}",
            containerFactory = "kafkaListenerManualCommitContainerFactory"
    )
    public void handleTransferCompleted(@Payload String message,
                                        ConsumerRecord<String, String> record,
                                        Acknowledgment acknowledgment) throws Exception {
        String messageId = String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
        try {
            if (kafkaEvents.containsKey(messageId)) {
                log.info("Дубликат пропущен [id={}]:",  messageId);
                acknowledgment.acknowledge();
                return;
            }

            log.info("Получено событие Kafka [id={}]: partition={}, offset={}",
                    messageId, record.partition(), record.offset());

            KafkaEventWrapper wrapper = objectMapper.readValue(message, KafkaEventWrapper.class);

            if (!"TRANSFER_COMPLETED".equals(wrapper.getEventType())) {
                log.debug("Пропускаем событие с типом: {}", wrapper.getEventType());
                acknowledgment.acknowledge();
                return;
            }

            if (wrapper.getData() == null) {
                log.error("Поле data пустое для TRANSFER_COMPLETED события");
                throw new IllegalArgumentException("Data is null for TRANSFER_COMPLETED event");
            }

            JsonNode dataNode = wrapper.getData();
            TransferCompletedEvent event = objectMapper.treeToValue(dataNode, TransferCompletedEvent.class);
            if (event.getUserId() == null) {
                log.error("Недостаточно данных: userId");
                throw new IllegalArgumentException("Missing required fields: userId");
            }

            Optional<NotificationEntity> notificationEntity = notificationRepository
                    .findFirstByUserIdOrderByCreatedAtDesc(event.getUserId());
            notificationEntity.ifPresent(entity -> notificationService.sendTransferNotification(
                    event.getUserId(),
                    entity.getRecipientEmail(),
                    event.getAmount(),
                    event.getFromAccountId(),
                    event.getToAccountId()
            ));
            kafkaEvents.put(messageId, wrapper);
            acknowledgment.acknowledge();
            log.info("Обработано событие по совершенному переводу для пользователя: userId={}, messageId={}",
                    event.getUserId(), messageId);
        } catch (Exception exception) {
            log.error("Не удалось обработать событие по совершенному переводу для пользователя: {}",
                    exception.getMessage(), exception);
            throw exception;
        }
    }
}
