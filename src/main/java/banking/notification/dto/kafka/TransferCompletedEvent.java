package banking.notification.dto.kafka;

import banking.notification.enums.Currency;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferCompletedEvent {
    private UUID userId;
    private BigDecimal amount;
    private Currency currency;
    private LocalDateTime occurredAt;
    private UUID toAccountId;
    private UUID fromAccountId;
    private UUID transactionId;
}
