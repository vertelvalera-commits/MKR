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

public class Consumer {

    private static final String BOOTSTRAP_SERVERS =
            Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS")).orElse("kafka:29092");
    private static final String TOPIC_REQUEST = "demo-requests";
    private static final String TOPIC_RESPONSE = "demo-responses";
    private static final String GROUP_ID = "demo-responder-group";
    private static final String CORRELATION_HEADER = "correlation-id";

    public static void main(String[] args) throws Exception {
        ensureTopics();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(TOPIC_REQUEST));
            System.out.println("Чекаю запитів у '" + TOPIC_REQUEST + "'.");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> r : records) {
                    String[] parts = r.value().split(",");
                    if (parts.length != 2) {
                        System.err.println("Невалідний запит: " + r.value());
                        continue;
                    }
                    int start = Integer.parseInt(parts[0].trim());
                    int finish = Integer.parseInt(parts[1].trim());
                    System.out.printf("<- Отримано запит: start=%d finish=%d%n", start, finish);

                    double avg = averageCollatzSteps(start, finish);

                    Header h = r.headers().lastHeader(CORRELATION_HEADER);
                    String corrId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : "";

                    String avgStr = formatAvg(avg);
                    ProducerRecord<String, String> response =
                            new ProducerRecord<>(TOPIC_RESPONSE, null, avgStr);
                    if (!corrId.isEmpty()) {
                        response.headers().add(CORRELATION_HEADER, corrId.getBytes(StandardCharsets.UTF_8));
                    }
                    producer.send(response).get();
                    System.out.printf("-> Надіслано відповідь: avgSteps=%s%n", avgStr);
                }
            }
        }
    }

    private static double averageCollatzSteps(int start, int finish) {
        if (finish < start) {
            int t = start; start = finish; finish = t;
        }
        long total = 0;
        long count = 0;
        for (int n = start; n <= finish; n++) {
            if (n >= 1) {
                total += collatzSteps(n);
                count++;
            }
        }
        return count == 0 ? 0.0 : (double) total / count;
    }

    private static int collatzSteps(int n) {
        long x = n;
        int steps = 0;
        while (x != 1) {
            if ((x & 1L) == 0L) x = x >> 1;
            else x = 3L * x + 1L;
            steps++;
        }
        return steps;
    }

    private static String formatAvg(double v) {
        if (v == Math.floor(v)) return String.valueOf((long) v);
        return String.format(java.util.Locale.ROOT, "%.4f", v);
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
