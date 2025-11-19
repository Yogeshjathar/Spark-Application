package com.learning.springboot_spark_pipeline.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("")
public class WelcomeController {

    @Value("${spring.application.name}")
    private String appName;

    @GetMapping("/welcome")
    public String welcome() {
        return "Welcome to " + appName + "...";
    }
}
