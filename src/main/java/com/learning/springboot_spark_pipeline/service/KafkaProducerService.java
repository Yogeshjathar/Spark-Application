package com.learning.springboot_spark_pipeline.service;

import com.learning.springboot_spark_pipeline.entity.SalesEvent;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String sendMessage(SalesEvent salesEvent) {
        kafkaTemplate.send("sales-topic", salesEvent);
        log.info("Produced message for product '{}' with quantity {} at a price of {}.",
                salesEvent.getProduct(),
                salesEvent.getQuantity(),
                salesEvent.getPrice());
        return "Message sent to Kafka";
    }
}
