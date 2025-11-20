package com.learning.springboot_spark_pipeline.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
public class BatchPipelineService {

    private final SparkSession spark;

    public BatchPipelineService(SparkSession spark) {
        this.spark = spark;
    }

    public String runBatchPipeline() {

        // 1️⃣ Read CSV file
        String path = "src/main/resources/inputData/sales_data.csv";
        
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);

        System.out.println("Input Data:");
        df.show();

        // 2️⃣ Clean — remove nulls
        Dataset<Row> cleaned = df.na().drop();

        // 3️⃣ Transformation
        Dataset<Row> result = cleaned
                .withColumn("totalAmount", col("quantity").multiply(col("price")))
                .groupBy("product")
                .agg(
                        sum("totalAmount").alias("totalRevenue"),
                        avg("totalAmount").alias("avgOrderValue")
                );

        System.out.println("TransAformed Result:");
        result.show();

        // 4️⃣ Write output to disk
        String outputPath = "src/main/resources/outputData/batch_result";
        
        result.coalesce(1)   // write single file
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        return "Batch pipeline completed — Output written to: " + outputPath;
    }
}
