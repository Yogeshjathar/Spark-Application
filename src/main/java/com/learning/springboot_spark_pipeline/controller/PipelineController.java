package com.learning.springboot_spark_pipeline.controller;

import com.learning.springboot_spark_pipeline.service.PipelineService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PipelineController {

    private final PipelineService pipelineService;

    public PipelineController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    // Trigger simple demo pipeline
    @GetMapping("/pipeline/run")
    public String runPipeline() {
        long result = pipelineService.runSimplePipeline();
        return "Pipeline executed successfully. Result: " + result;
    }
}
