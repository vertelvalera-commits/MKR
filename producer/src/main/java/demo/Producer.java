package demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Producer {

    private static final String BOOTSTRAP_SERVERS =
            Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS")).orElse("kafka:29092");
    private static final String TOPIC_REQUEST = "demo-requests";
    private static final String TOPIC_RESPONSE = "demo-responses";
    private static final String CORRELATION_HEADER = "correlation-id";

    private static final int START = 10;
    private static final int FINISH = 100;
    private static final long REPLY_TIMEOUT_MS = 3_000_000L;

    public static void main(String[] args) throws Exception {
        ensureTopics();

        String correlationId = UUID.randomUUID().toString();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-requester-" + correlationId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC_RESPONSE));
            long assignDeadline = System.currentTimeMillis() + 10_000L;
            while (consumer.assignment().isEmpty() && System.currentTimeMillis() < assignDeadline) {
                consumer.poll(Duration.ofMillis(200));
            }

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                String payload = START + "," + FINISH;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_REQUEST, null, payload);
                record.headers().add(CORRELATION_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
                producer.send(record).get();
                System.out.printf("-> Запит надіслано: start=%d finish=%d (id=%s)%n",
                        START, FINISH, correlationId);
            }

            long deadline = System.currentTimeMillis() + REPLY_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> r : records) {
                    Header h = r.headers().lastHeader(CORRELATION_HEADER);
                    if (h == null) continue;
                    String id = new String(h.value(), StandardCharsets.UTF_8);
                    if (!correlationId.equals(id)) continue;

                    System.out.println("<- Отримано відповідь: avgSteps=" + r.value());
                    System.out.println("Готово. Контейнер живе.");
                    new CountDownLatch(1).await();
                    return;
                }
            }
            System.err.println("Timeout: відповідь не отримано за " + REPLY_TIMEOUT_MS + " мс");
            new CountDownLatch(1).await();
        }
    }

    private static void ensureTopics() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : List.of(TOPIC_REQUEST, TOPIC_RESPONSE)) {
                if (!existing.contains(t)) {
                    toCreate.add(new NewTopic(t, 1, (short) 1));
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get();
            }
        }
    }
}
