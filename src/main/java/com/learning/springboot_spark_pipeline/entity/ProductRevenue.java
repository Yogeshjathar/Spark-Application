package com.learning.springboot_spark_pipeline.entity;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data  
@Table(name = "product_revenue")
public class ProductRevenue {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String product;
    private Double totalRevenue;
    private Double avgOrderValue;

    // getter and setters
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(Double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public Double getAvgOrderValue() {
        return avgOrderValue;
    }

    public void setAvgOrderValue(Double avgOrderValue) {
        this.avgOrderValue = avgOrderValue;
    }
}
