package com.example.demo_bigdata_imdb.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.stereotype.Component;

@Component
@EnableKafka
public class MovieDataConsumer {

    // Đánh dấu Kafka Listener để nhận dữ liệu từ Kafka topic
    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "imdb-data-group")
    public void consumeMessage(ConsumerRecord<String, String> record) {
        String message = record.value();
        System.out.println("Received message: " + message);

        // Sau khi nhận dữ liệu, bạn có thể xử lý và lưu vào database
        processMovieData(message);
    }

    // Phương thức để xử lý dữ liệu và lưu vào database
    private void processMovieData(String data) {
        // Ví dụ: parse dữ liệu và lưu vào cơ sở dữ liệu.
        System.out.println("Processing data: " + data);
    }
}
