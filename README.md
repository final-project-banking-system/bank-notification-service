# Notification Service (Сервис Notification)

Notification Service — сервис по работе с клиентскими уведомлениями банковской системы.
Отвечает за обработку банковских событий и передачу информации о них клиенту через email-уведомления.
Отправка по email реализована через Unisender API (на данный момент сервис по работе с API замокан).

---

## Основные возможности

- Отправка клиенту уведомлений посредством вычитки событий из топиков Kafka
- Обработка шаблонов и их использование для генерации письма-уведомления

---

## Используемые технологии

- Java 17
- Spring Boot 3
- Spring Security
- Spring Data JPA
- PostgreSQL
- Liquibase
- Apache Kafka
- Docker / Docker Compose
- Unisender API
- Rest Client

---

## Используемые Kafka топики

- `auth.users` — события по созданию профиля клиента (USER_CREATED)
- `auth.logins` — события по аутентификации пользователя (USER_LOGIN)
- `banking.transfers` — события по переводам между счетами (TRANSFER_COMPLETED)
- `system.errors` — системные ошибки сервиса (SYSTEM_ERROR)

---

## Как запустить локально

### Запуск через Docker Compose

1. Поднять инфраструктуру и сервисы:

```bash
docker compose up -d
```

2. Проверить логи Notification Service:

```bash
docker logs -f notification-service
```

## Обрабатываемые события Kafka

### Регистрация пользователя

#### Пример события
```json
{
  "data": {
    "role": "USER",
    "email": "user1@mail.com",
    "login": "user1",
    "userId": "77b6adb2-730f-49de-b1f2-98a2bd4f275f"
  },
  "eventType": "USER_CREATED"
}
```

### Аутентификация пользователя

#### Пример события
```json
{
  "data": {
    "login": "user1",
    "userId": "77b6adb2-730f-49de-b1f2-98a2bd4f275f",
    "deviceInfo": "local-test",
    "occurredAt": "2026-01-23T13:15:07.615507131"
  },
  "eventType": "USER_LOGIN"
}
```

### Перевод между счетами

#### Пример события
```json
{
  "data": {
    "amount": 15,
    "userId": "77b6adb2-730f-49de-b1f2-98a2bd4f275f",
    "currency": "RUB",
    "occurredAt": "2026-01-23T13:15:08.015184339",
    "toAccountId": "7a741c7c-ff28-442c-aa3a-5e4bf0e103be",
    "fromAccountId": "062cdbf8-6a4c-4cae-aa3c-72019286948c",
    "transactionId": "e78f44a8-5599-44d5-ac71-0a4ad7937c56"
  },
  "eventType": "TRANSFER_COMPLETED"
}
```

## Примечания

- Liquibase используется как **единственный источник истины** схемы базы данных.
