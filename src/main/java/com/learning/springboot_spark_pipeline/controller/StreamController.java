package com.learning.springboot_spark_pipeline.controller;

import com.learning.springboot_spark_pipeline.service.StreamPipelineService;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/stream")
public class StreamController {

    private final StreamPipelineService service;

    public StreamController(StreamPipelineService service) {
        this.service = service;
    }

    @GetMapping("/start")
    public String startStream() {
        new Thread(() -> {
            try {
                service.startStreamingPipeline();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            } catch (StreamingQueryException e) {
                throw new RuntimeException(e);
            }
        }).start();
        return "Streaming Pipeline Started!";
    }
}
