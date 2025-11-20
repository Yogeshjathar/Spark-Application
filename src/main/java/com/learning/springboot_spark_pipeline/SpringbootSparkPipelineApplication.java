package com.learning.springboot_spark_pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SpringbootSparkPipelineApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringbootSparkPipelineApplication.class, args);
	}

}
