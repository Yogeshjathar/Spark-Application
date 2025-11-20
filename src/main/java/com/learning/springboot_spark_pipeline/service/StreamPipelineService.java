package com.learning.springboot_spark_pipeline.service;

import com.learning.springboot_spark_pipeline.entity.ProductRevenue;
import com.learning.springboot_spark_pipeline.repo.ProductRevenueRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

@Service
public class StreamPipelineService {

    private final SparkSession spark;
    private final ProductRevenueRepository productRevenueRepository;

    public StreamPipelineService(SparkSession spark, ProductRevenueRepository productRevenueRepository) {
        this.spark = spark;
        this.productRevenueRepository = productRevenueRepository;
    }

    public void startStreamingPipeline() throws TimeoutException, StreamingQueryException {

        // 1. Read stream from Kafka
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "sales-topic")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false") // avoid offset missing exception
                .load();

        // 2. Convert Kafka "value" -> String
        Dataset<Row> stringDF = df.selectExpr("CAST(value AS STRING)");

        // JSON parsing
        StructType schema = new StructType()
                .add("product", DataTypes.StringType)
                .add("quantity", DataTypes.IntegerType)
                .add("price", DataTypes.DoubleType);

        Dataset<Row> sales = stringDF
                .select(from_json(col("value"), schema).as("data"))
                .select("data.*");


        // 4. Transformation
        Dataset<Row> result = sales
                .withColumn("totalAmount", col("quantity").multiply(col("price")))
                .groupBy("product")
                .agg(
                        sum("totalAmount").alias("totalRevenue"),
                        avg("totalAmount").alias("avgOrderValue")
                );

        result.printSchema();

        // 5. Write to DB using foreachBatch
        result.writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {
                    List<Row> rows = batchDF.collectAsList();
                    List<ProductRevenue> list = new ArrayList<>();

                    for (Row r : rows) {
                        ProductRevenue pr = new ProductRevenue();
                        pr.setProduct(r.getAs("product"));
                        pr.setTotalRevenue(r.getAs("totalRevenue"));
                        pr.setAvgOrderValue(r.getAs("avgOrderValue"));
                        list.add(pr);
                    }

                    if (!list.isEmpty()) {
                        productRevenueRepository.saveAll(list);
                    }

                })
                .option("checkpointLocation", "checkpoint/stream1")
                .start()
                .awaitTermination();
    }
}
