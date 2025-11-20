package com.learning.springboot_spark_pipeline.repo;

import com.learning.springboot_spark_pipeline.entity.ProductRevenue;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRevenueRepository extends JpaRepository<ProductRevenue, Long> {
}
