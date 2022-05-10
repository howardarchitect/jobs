package com.roark;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class SpringJobsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringJobsApplication.class, args);
	}
	


	@Autowired
	JobBuilderFactory jobs;
	
	
	@Autowired
	StepBuilderFactory steps;

	

	public Tasklet helloWorldTasklet() {
		return (new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Hello world");
				return RepeatStatus.FINISHED;
			}
		});
	}

	@Bean
	public Step step1() {
		return steps.get("step1").tasklet(helloWorldTasklet()).build();
	}

	@Bean
	public Job helloWorldJob() {
		return jobs.get("helloWorldJob").start(step1()).build();
	}



}
