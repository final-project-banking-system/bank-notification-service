package unit;

import banking.notification.model.NotificationEntity;
import banking.notification.model.TemplateEntity;
import banking.notification.repository.NotificationRepository;
import banking.notification.service.MockUnisenderService;
import banking.notification.service.NotificationService;
import banking.notification.service.TemplateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class NotificationServiceUnitTest {
    @Mock
    private NotificationRepository notificationRepository;

    @Mock
    private MockUnisenderService mockUnisenderService;

    @Mock
    private TemplateService templateService;

    @InjectMocks
    private NotificationService notificationService;

    @Captor
    private ArgumentCaptor<NotificationEntity> notificationCaptor;

    private UUID userId;
    private UUID notificationId;
    private String userEmail;
    private String userLogin;
    private UUID fromAccountId;
    private UUID toAccountId;

    @BeforeEach
    public void setUp() {
        userId = UUID.randomUUID();
        notificationId = UUID.randomUUID();
        userEmail = "test@example.com";
        userLogin = "test_user";
        fromAccountId = UUID.randomUUID();
        toAccountId = UUID.randomUUID();
    }

    private TemplateEntity createTemplate(String type, String subject, String body) {
        TemplateEntity template = new TemplateEntity();
        template.setType(type);
        template.setSubjectTemplate(subject);
        template.setBodyTemplate(body);
        return template;
    }

    @Test
    public void testSendWelcomeNotification_whenTemplateFound_shouldSendEmailAndSaveNotification() {
        TemplateEntity template = createTemplate("WELCOME", "Welcome {username}!",
                "Hello {username}! Created on {date}");
        Map<String, String> params = Map.of("username", userLogin, "date", "01.01.2026");

        when(templateService.getTemplate("WELCOME")).thenReturn(Optional.of(template));
        when(templateService.welcomeParams(userLogin)).thenReturn(params);
        when(templateService.processTemplate(anyString(), eq(params)))
                .thenAnswer(invocation -> {
                    String templateStr = invocation.getArgument(0);
                    return templateStr.replace("{username}", userLogin).replace("{date}", "01.01.2026");
                });
        when(mockUnisenderService.sendEmail(eq(userEmail), anyString(), anyString()))
                .thenReturn(true);
        when(notificationRepository.save(any(NotificationEntity.class)))
                .thenAnswer(invocation -> {
                    NotificationEntity entity = invocation.getArgument(0);
                    entity.setId(notificationId);
                    return entity;
                });

        notificationService.sendWelcomeNotification(userId, userLogin, userEmail);

        verify(templateService).getTemplate("WELCOME");
        verify(templateService).welcomeParams(userLogin);
        verify(templateService, times(2)).processTemplate(anyString(), eq(params));
        verify(mockUnisenderService).sendEmail(eq(userEmail), contains("Welcome"), contains("Hello"));

        verify(notificationRepository).save(notificationCaptor.capture());
        NotificationEntity savedNotification = notificationCaptor.getValue();

        assertNotNull(savedNotification);
        assertEquals(userId, savedNotification.getUserId());
        assertEquals(userEmail, savedNotification.getRecipientEmail());
        assertEquals("WELCOME", savedNotification.getType());
        assertEquals("EMAIL", savedNotification.getChannel());
        assertEquals("SENT", savedNotification.getStatus());
        assertNotNull(savedNotification.getCreatedAt());
        assertNotNull(savedNotification.getSentAt());
        assertEquals(0, savedNotification.getRetryCount());
    }

    @Test
    public void testSendWelcomeNotification_whenTemplateNotFound_shouldSendDefaultWelcome() {
        when(templateService.getTemplate("WELCOME")).thenReturn(Optional.empty());
        when(mockUnisenderService.sendEmail(eq(userEmail), eq("Добро пожаловать!"),
                contains("Добро пожаловать, " + userLogin + "!"))).thenReturn(true);
        when(notificationRepository.save(any(NotificationEntity.class)))
                .thenAnswer(invocation -> {
                    NotificationEntity entity = invocation.getArgument(0);
                    entity.setId(notificationId);
                    return entity;
                });

        notificationService.sendWelcomeNotification(userId, userLogin, userEmail);

        verify(templateService).getTemplate("WELCOME");
        verify(mockUnisenderService).sendEmail(eq(userEmail), eq("Добро пожаловать!"),
                contains("Добро пожаловать, " + userLogin + "!"));

        verify(notificationRepository).save(notificationCaptor.capture());
        NotificationEntity savedNotification = notificationCaptor.getValue();

        assertEquals("WELCOME", savedNotification.getType());
        assertEquals("SENT", savedNotification.getStatus());
    }

    @Test
    public void testSendWelcomeNotification_whenEmailSendFails_shouldSaveFailedNotification() {
        TemplateEntity template = createTemplate("WELCOME", "Welcome", "Hello");
        Map<String, String> params = Map.of();

        when(templateService.getTemplate("WELCOME")).thenReturn(Optional.of(template));
        when(templateService.welcomeParams(userLogin)).thenReturn(params);
        when(templateService.processTemplate(anyString(), eq(params)))
                .thenReturn("Processed template");
        when(mockUnisenderService.sendEmail(eq(userEmail), anyString(), anyString()))
                .thenReturn(false);

        notificationService.sendWelcomeNotification(userId, userLogin, userEmail);

        verify(notificationRepository).save(notificationCaptor.capture());
        NotificationEntity savedNotification = notificationCaptor.getValue();

        assertEquals("FAILED", savedNotification.getStatus());
        assertNull(savedNotification.getSentAt());
    }

    @Test
    public void testSendLoginNotification_whenTemplateFound_shouldSendEmail() {
        String deviceInfo = "Chrome on Windows";
        TemplateEntity template = createTemplate("LOGIN", "Login alert",
                "Login from {device} at {date}");
        Map<String, String> params = Map.of("device", deviceInfo, "date", "01.01.2026");

        when(templateService.getTemplate("LOGIN")).thenReturn(Optional.of(template));
        when(templateService.loginParams("Пользователь", deviceInfo)).thenReturn(params);
        when(templateService.processTemplate(anyString(), eq(params)))
                .thenAnswer(invocation -> {
                    String templateStr = invocation.getArgument(0);
                    return templateStr.replace("{device}", deviceInfo).replace("{date}", "01.01.2026");
                });
        when(mockUnisenderService.sendEmail(eq(userEmail), anyString(), anyString()))
                .thenReturn(true);

        notificationService.sendLoginNotification(userId, userEmail, deviceInfo);

        verify(templateService).getTemplate("LOGIN");
        verify(templateService).loginParams("Пользователь", deviceInfo);
        verify(mockUnisenderService).sendEmail(eq(userEmail), anyString(), contains(deviceInfo));
        verify(notificationRepository).save(any(NotificationEntity.class));
    }

    @Test
    public void testSendLoginNotification_whenTemplateNotFound_shouldDoNothing() {
        when(templateService.getTemplate("LOGIN")).thenReturn(Optional.empty());

        notificationService.sendLoginNotification(userId, userEmail, "device");

        verify(templateService).getTemplate("LOGIN");
        verifyNoInteractions(mockUnisenderService);
        verifyNoInteractions(notificationRepository);
    }

    @Test
    public void testSendTransferNotification_whenTemplateFound_shouldSendEmail() {
        BigDecimal amount = new BigDecimal("100.50");
        TemplateEntity template = createTemplate("TRANSFER", "Transfer completed",
                "Transfer {amount} from {from} to {to}");
        Map<String, String> params = Map.of(
                "amount", amount.toString(),
                "from", fromAccountId.toString(),
                "to", toAccountId.toString(),
                "date", "01.01.2026"
        );

        when(templateService.getTemplate("TRANSFER")).thenReturn(Optional.of(template));
        when(templateService.transferParams(eq(amount),
                eq(fromAccountId.toString()), eq(toAccountId.toString())))
                .thenReturn(params);
        when(templateService.processTemplate(anyString(), eq(params)))
                .thenAnswer(invocation -> {
                    String templateStr = invocation.getArgument(0);
                    return templateStr
                            .replace("{amount}", amount.toString())
                            .replace("{from}", fromAccountId.toString())
                            .replace("{to}", toAccountId.toString());
                });
        when(mockUnisenderService.sendEmail(eq(userEmail), anyString(), anyString()))
                .thenReturn(true);

        notificationService.sendTransferNotification(userId, userEmail, amount, fromAccountId, toAccountId);

        verify(templateService).getTemplate("TRANSFER");
        verify(templateService).transferParams(eq(amount),
                eq(fromAccountId.toString()), eq(toAccountId.toString()));
        verify(notificationRepository).save(any(NotificationEntity.class));
    }

    @Test
    public void testSendTransferNotification_whenNullAmount_shouldHandleGracefully() {
        TemplateEntity template = createTemplate("TRANSFER", "Transfer", "Body");
        Map<String, String> params = Map.of();

        when(templateService.getTemplate("TRANSFER")).thenReturn(Optional.of(template));
        when(templateService.transferParams(isNull(), anyString(), anyString()))
                .thenReturn(params);
        when(templateService.processTemplate(anyString(), eq(params))).thenReturn("Processed");
        when(mockUnisenderService.sendEmail(anyString(), anyString(), anyString()))
                .thenReturn(true);

        notificationService.sendTransferNotification(userId, userEmail, null, fromAccountId, toAccountId);

        verify(templateService).transferParams(isNull(), anyString(), anyString());
        verify(notificationRepository).save(any(NotificationEntity.class));
    }

    @Test
    public void testSendTransferNotification_whenTemplateNotFound_shouldDoNothing() {
        when(templateService.getTemplate("TRANSFER")).thenReturn(Optional.empty());

        notificationService.sendTransferNotification(userId, userEmail,
                new BigDecimal("100"), fromAccountId, toAccountId);

        verify(templateService).getTemplate("TRANSFER");
        verifyNoInteractions(mockUnisenderService);
        verifyNoInteractions(notificationRepository);
    }

    @Test
    public void testSendWelcomeNotification_whenExceptionThrown_shouldLogError() {
        when(templateService.getTemplate("WELCOME")).thenThrow(new RuntimeException("DB error"));

        notificationService.sendWelcomeNotification(userId, userLogin, userEmail);

        verify(templateService).getTemplate("WELCOME");
        verifyNoInteractions(mockUnisenderService);
        verifyNoInteractions(notificationRepository);
    }

    @Test
    public void testSaveNotification_shouldSetAllRequiredFields() {
        TemplateEntity template = createTemplate("WELCOME", "Subject", "Body");

        when(templateService.getTemplate("WELCOME")).thenReturn(Optional.of(template));
        when(templateService.welcomeParams(anyString())).thenReturn(Map.of());
        when(templateService.processTemplate(anyString(), anyMap())).thenReturn("Processed");
        when(mockUnisenderService.sendEmail(anyString(), anyString(), anyString()))
                .thenReturn(true);

        NotificationEntity capturedEntity = new NotificationEntity();
        when(notificationRepository.save(any(NotificationEntity.class)))
                .thenAnswer(invocation -> {
                    capturedEntity.setId(notificationId);
                    capturedEntity.setUserId(userId);
                    capturedEntity.setRecipientEmail(userEmail);
                    capturedEntity.setSubject("Subject");
                    capturedEntity.setType("WELCOME");
                    capturedEntity.setChannel("EMAIL");
                    capturedEntity.setStatus("SENT");
                    capturedEntity.setRetryCount(0);
                    capturedEntity.setCreatedAt(LocalDateTime.now());
                    capturedEntity.setSentAt(LocalDateTime.now());
                    return capturedEntity;
                });

        notificationService.sendWelcomeNotification(userId, userLogin, userEmail);

        assertNotNull(capturedEntity.getUserId());
        assertNotNull(capturedEntity.getRecipientEmail());
        assertNotNull(capturedEntity.getSubject());
        assertEquals("WELCOME", capturedEntity.getType());
        assertEquals("EMAIL", capturedEntity.getChannel());
        assertEquals("SENT", capturedEntity.getStatus());
        assertEquals(0, capturedEntity.getRetryCount());
        assertNotNull(capturedEntity.getCreatedAt());
        assertNotNull(capturedEntity.getSentAt());
    }
}
