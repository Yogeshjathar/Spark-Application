package com.learning.springboot_spark_pipeline.controller;

import com.learning.springboot_spark_pipeline.service.BatchPipelineService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchPipelineController {

    private final BatchPipelineService pipelineService;

    public BatchPipelineController(BatchPipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @GetMapping("/run-batch")
    public String runBatchPipeline() {
        return pipelineService.runBatchPipeline();
    }
}
