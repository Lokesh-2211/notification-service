# Notification Service (learning project)

A notification service built step by step to practice Java architecture.

## Architecture so far (Step 2)

```
Other service --POST--> Notification API --> Kafka topic "notifications"
                        (Spring Boot)          (3 partitions)
```

## Run it

1. Start infrastructure: `docker compose up -d`
2. Open the project in IntelliJ and run `NotificationServiceApplication`
   (or from the terminal: `mvn spring-boot:run`)
3. Send a notification:

```
curl -X POST http://localhost:8080/api/notifications \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","channel":"EMAIL","type":"PASSWORD_RESET","recipient":"loki@example.com","message":"Reset your password"}'
```

4. See it in Kafka UI: http://localhost:8090 → Topics → notifications → Messages

## Design decisions

- **API returns 202 Accepted, not 200 OK.** The API only queues the notification.
  Sending happens later, so callers never wait on email/SMS providers.
- **Kafka between API and senders.** Absorbs traffic spikes (promo campaigns)
  and keeps notifications safe if a sender is down.
- **Message key = userId.** Same key goes to the same partition, so one user's
  notifications stay in order.
- **Every notification gets a UUID.** Used later to prevent duplicate sends.

## Roadmap

- [x] Step 1: Kafka + Postgres with Docker
- [x] Step 2: API that publishes to Kafka
- [ ] Step 3: Worker that consumes and "sends"
- [ ] Step 4: Save status in Postgres
- [ ] Step 5: Skip duplicates (idempotency)
- [ ] Step 6: Urgent vs bulk topics
- [ ] Step 7: Retries + dead letter topic
- [ ] Step 8: Send a real email
