package com.learning.springboot_spark_pipeline.controller;

import com.learning.springboot_spark_pipeline.entity.SalesEvent;
import com.learning.springboot_spark_pipeline.service.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducerService producerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/publish")
    public String publishMessage(@RequestBody SalesEvent salesEvent) {
        return producerService.sendMessage(salesEvent);
    }
}
