package com.learning.springboot_spark_pipeline.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

/*    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("SpringSparkApp")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
    }*/

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession() {

        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        System.setProperty("HADOOP_HOME", "C:\\hadoop");

        SparkConf conf = new SparkConf()
                .setAppName("SpringBootSparkApp")
                .setMaster("local[*]")
                // disable UI and metrics (fixes javax.servlet error)
                .set("spark.ui.enabled", "false")
                .set("spark.ui.showConsoleProgress", "false")
                .set("spark.metrics.conf.*", "")
                .set("spark.metrics.namespace", "ignore");

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @Bean(destroyMethod = "close")
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        return new JavaSparkContext(sparkSession.sparkContext());
    }
}
