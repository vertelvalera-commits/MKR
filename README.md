# Kafka MKR — request–reply (Java)

Два сервіси `producer` та `consumer` спілкуються через Apache Kafka двома топіками:

```
producer  ── demo-requests ──► consumer
producer  ◄── demo-responses ── consumer
```

`producer` шле `"start,finish"` (за замовчуванням `10,100`), `consumer` рахує **середню кількість кроків послідовностей Колатца** для діапазону `[start..finish]` і відповідає в `demo-responses`. Відповідь матчиться по заголовку `correlation-id` (UUID).

## Стек

- Java 17
- `org.apache.kafka:kafka-clients` 3.7.0
- Maven (shade-плагін → fat-jar)
- Docker (KRaft Kafka, без Zookeeper)

## Запуск (Windows / Docker Desktop)

```powershell
# 1. Мережа
docker network create kafka-net

# 2. Kafka (KRaft, без Zookeeper)
docker run -d --name kafka --network kafka-net -p 9092:9092 `
  -e KAFKA_NODE_ID=1 `
  -e KAFKA_PROCESS_ROLES=broker,controller `
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 `
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9092 `
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 `
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT `
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT `
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 `
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true `
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk `
  -v kafka-data:/var/lib/kafka/data `
  confluentinc/cp-kafka:7.7.1

# 3. Образи
docker build -t kafka-demo-consumer ./consumer
docker build -t kafka-demo-producer ./producer

# 4. Запуск сервісів
docker run -d --name kafka-consumer --network kafka-net kafka-demo-consumer
docker run -d --name kafka-producer --network kafka-net kafka-demo-producer

# 5. Логи
docker logs kafka-consumer
docker logs kafka-producer
```

## Очікувані логи

**Producer:**
```
-> Запит надіслано: start=10 finish=100 (id=...)
<- Отримано відповідь: avgSteps=...
Готово. Контейнер живе.
```

**Consumer:**
```
Чекаю запитів у 'demo-requests'.
<- Отримано запит: start=10 finish=100
-> Надіслано відповідь: avgSteps=...
```

## Конфігурація

Усі параметри (топіки, group-id, діапазон `start..finish`) — константи в коді. Єдина змінна середовища — `BOOTSTRAP_SERVERS`:

| Контекст | Значення |
|---|---|
| У Docker (мережа `kafka-net`) | `kafka:29092` (default) |
| Локально (host) | `localhost:9092` |

## Очищення

```powershell
docker rm -f kafka-producer kafka-consumer kafka
docker network rm kafka-net
docker volume rm kafka-data
```
