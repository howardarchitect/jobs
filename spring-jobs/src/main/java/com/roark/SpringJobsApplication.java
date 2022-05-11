package com.roark;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SpringJobsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringJobsApplication.class, args);
	}
	




}
