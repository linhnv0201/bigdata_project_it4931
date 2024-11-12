package com.example.demo_bigdata_imdb.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

@Component
public class FileProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Constructor injection
    public FileProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Hàm này sẽ gửi dữ liệu từ file .tsv vào Kafka topic
    public void sendFileData(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Gửi từng dòng vào Kafka topic
                kafkaTemplate.send(topic, line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Error reading file", e);
        }
    }
}
