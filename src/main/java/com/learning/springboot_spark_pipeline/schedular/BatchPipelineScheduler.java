package com.learning.springboot_spark_pipeline.schedular;

import com.learning.springboot_spark_pipeline.service.BatchPipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BatchPipelineScheduler {

    private static final Logger log = LoggerFactory.getLogger(BatchPipelineScheduler.class);
    private final BatchPipelineService batchPipelineService;

    public BatchPipelineScheduler(BatchPipelineService batchPipelineService) {
        this.batchPipelineService = batchPipelineService;
    }

    @Scheduled(cron = "0 3 13 * * ?")
    public void runScheduledPipeline() {
        log.info("Scheduled Batch Pipeline Started...");
        batchPipelineService.runBatchPipeline();
        log.info("Scheduled Batch Pipeline Completed!");
    }
}
