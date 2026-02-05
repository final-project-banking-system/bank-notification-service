package banking.notification.service;

import banking.notification.model.NotificationEntity;
import banking.notification.model.TemplateEntity;
import banking.notification.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class NotificationService {
    private final NotificationRepository notificationRepository;
    private final MockUnisenderService mockUnisenderService;
    private final TemplateService templateService;

    @Transactional
    public void sendWelcomeNotification(UUID userId, String login, String email) {
        try {
            log.info("Отправка welcome для userId: {}", userId);

            Optional<TemplateEntity> templateOpt = templateService.getTemplate("WELCOME");
            if (templateOpt.isEmpty()) {
                log.warn("Шаблон WELCOME не найден, используем дефолтный");
                sendDefaultWelcome(userId, login, email);
                return;
            }

            TemplateEntity template = templateOpt.get();
            Map<String, String> params = templateService.welcomeParams(login);

            String subject = templateService.processTemplate(template.getSubjectTemplate(), params);
            String body = templateService.processTemplate(template.getBodyTemplate(), params);

            boolean sent = mockUnisenderService.sendEmail(email, subject, body);

            saveNotification(userId, email, subject, sent, "WELCOME");

            log.info("Welcome отправлен: userId={}, email={}", userId, email);

        } catch (Exception e) {
            log.error("Ошибка отправки welcome: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Transactional
    public void sendLoginNotification(UUID userId, String email, String deviceInfo) {
        try {
            log.info("Отправка login для userId: {}", userId);

            Optional<TemplateEntity> templateOpt = templateService.getTemplate("LOGIN");
            if (templateOpt.isEmpty()) {
                log.warn("Шаблон LOGIN не найден, пропускаем");
                return;
            }

            TemplateEntity template = templateOpt.get();
            Map<String, String> params = templateService.loginParams("Пользователь", deviceInfo);

            String subject = templateService.processTemplate(template.getSubjectTemplate(), params);
            String body = templateService.processTemplate(template.getBodyTemplate(), params);

            boolean sent = mockUnisenderService.sendEmail(email, subject, body);

            saveNotification(userId, email, subject, sent, "LOGIN");

            log.info("Login отправлен: userId={}, email={}", userId, email);

        } catch (Exception e) {
            log.error("Ошибка отправки login: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Transactional
    public void sendTransferNotification(UUID userId, String email,
                                         BigDecimal amount, UUID fromAccount, UUID toAccount) {
        try {
            log.info("Отправка transfer для userId: {}", userId);

            Optional<TemplateEntity> templateOpt = templateService.getTemplate("TRANSFER");
            if (templateOpt.isEmpty()) {
                log.warn("Шаблон TRANSFER не найден, пропускаем");
                return;
            }

            TemplateEntity template = templateOpt.get();
            Map<String, String> params = templateService.transferParams(amount,
                    fromAccount.toString(), toAccount.toString());

            String subject = templateService.processTemplate(template.getSubjectTemplate(), params);
            String body = templateService.processTemplate(template.getBodyTemplate(), params);

            boolean sent = mockUnisenderService.sendEmail(email, subject, body);

            saveNotification(userId, email, subject, sent, "TRANSFER");

            log.info("Transfer отправлен: userId={}, email={}", userId, email);

        } catch (Exception e) {
            log.error("Ошибка отправки transfer: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void sendDefaultWelcome(UUID userId, String login, String email) {
        String subject = "Добро пожаловать!";
        String body = "Добро пожаловать, " + login + "!";
        boolean sent = mockUnisenderService.sendEmail(email, subject, body);
        saveNotification(userId, email, subject, sent, "WELCOME");
    }

    private void saveNotification(UUID userId, String email, String subject, boolean sent, String type) {
        NotificationEntity notification = new NotificationEntity();
        notification.setUserId(userId);
        notification.setRecipientEmail(email);
        notification.setSubject(subject);
        notification.setType(type);
        notification.setChannel("EMAIL");
        notification.setStatus(sent ? "SENT" : "FAILED");
        notification.setRetryCount(0);
        notification.setCreatedAt(LocalDateTime.now());
        if (sent) {
            notification.setSentAt(LocalDateTime.now());
        }
        notificationRepository.save(notification);
    }
}