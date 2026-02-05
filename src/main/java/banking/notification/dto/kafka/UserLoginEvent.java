package banking.notification.dto.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserLoginEvent {
    private UUID userId;
    private String login;
    private String deviceInfo;
    private LocalDateTime occurredAt;
}
