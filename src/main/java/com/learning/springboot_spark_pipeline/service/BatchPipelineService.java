package com.learning.springboot_spark_pipeline.service;

import com.learning.springboot_spark_pipeline.entity.ProductRevenue;
import com.learning.springboot_spark_pipeline.repo.ProductRevenueRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@Service
public class BatchPipelineService {

    private final SparkSession spark;
    private final ProductRevenueRepository productRevenueRepository;

    public BatchPipelineService(SparkSession spark, ProductRevenueRepository productRevenueRepository) {
        this.spark = spark;
        this.productRevenueRepository = productRevenueRepository;
    }

    public String runBatchPipeline() {

        // Read CSV file
        String path = "src/main/resources/inputData/sales_data.csv";

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);

        System.out.println("Input Data:");
        df.show();

        // Clean — remove nulls
        Dataset<Row> cleaned = df.na().drop();

        // Transformation
        Dataset<Row> result = cleaned
                .withColumn("totalAmount", col("quantity").multiply(col("price")))
                .groupBy("product")
                .agg(
                        sum("totalAmount").alias("totalRevenue"),
                        avg("totalAmount").alias("avgOrderValue")
                );

        System.out.println("TransAformed Result:");
        result.show();

        List<Row> rows = result.collectAsList();
        List<ProductRevenue> list = new ArrayList<>();

        for(Row r : rows){
            ProductRevenue pr = new ProductRevenue();

            pr.setProduct(r.getString(0));
            pr.setTotalRevenue(((Number) r.get(1)).doubleValue());
            pr.setAvgOrderValue(((Number) r.get(2)).doubleValue());

            list.add(pr);
        }

        productRevenueRepository.saveAll(list);

        return "Data stored into the DB successfully !!!" ;
    }
}
