package com.learning.springboot_spark_pipeline.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.sum;

@Service
public class PipelineService {

    private final SparkSession spark;

    public PipelineService(SparkSession spark) {
        this.spark = spark;
    }


    public long runSimplePipeline() {

        // Sample input data
        List<Integer> sample = Arrays.asList(10, 20, 30, 40);

        Dataset<Row> df = spark
                .createDataset(sample, org.apache.spark.sql.Encoders.INT())
                .toDF("value");

        df.show(); // Debugging

        Row result = df.agg(sum("value").alias("total")).first();

        return result.getLong(0);
    }
}
