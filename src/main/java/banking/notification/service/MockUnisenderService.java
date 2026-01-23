package banking.notification.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MockUnisenderService {
    public boolean sendEmail(String recipientEmail, String subject, String body) {
        log.info("[MOCK] Отправка email: to={}, subject={}, body length={}",
                recipientEmail, subject, body.length());
        log.info("[MOCK] Email body: {}", body);
        return true;
    }
}
