package banking.notification.service;

import banking.notification.model.TemplateEntity;
import banking.notification.repository.TemplateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TemplateService {

    private final TemplateRepository templateRepository;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

    /**
     * Получить шаблон по типу
     */
    public Optional<TemplateEntity> getTemplate(String type) {
        return templateRepository.findByType(type);
    }

    /**
     * Обработать шаблон с параметрами
     */
    public String processTemplate(String template, Map<String, String> params) {
        if (template == null || params == null) return template;

        String result = template;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            result = result.replace("{" + entry.getKey() + "}",
                    entry.getValue() != null ? entry.getValue() : "");
        }
        return result;
    }

    /**
     * Простые параметры для welcome email
     */
    public Map<String, String> welcomeParams(String username) {
        Map<String, String> params = new HashMap<>();
        params.put("username", username);
        params.put("date", LocalDateTime.now().format(DATE_FORMATTER));
        return params;
    }

    /**
     * Простые параметры для login email
     */
    public Map<String, String> loginParams(String username, String deviceInfo) {
        Map<String, String> params = new HashMap<>();
        params.put("username", username);
        params.put("date", LocalDateTime.now().format(DATE_FORMATTER));
        params.put("device", deviceInfo != null ? deviceInfo : "неизвестное устройство");
        return params;
    }

    /**
     * Простые параметры для transfer email
     */
    public Map<String, String> transferParams(BigDecimal amount, String fromAccount, String toAccount) {
        Map<String, String> params = new HashMap<>();
        params.put("amount", amount != null ? amount.toString() : "0");
        params.put("from", fromAccount);
        params.put("to", toAccount);
        params.put("date", LocalDateTime.now().format(DATE_FORMATTER));
        return params;
    }
}