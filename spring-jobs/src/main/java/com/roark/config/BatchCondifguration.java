package com.roark.config;

import com.roark.listener.HwJobExecutionListener;
import com.roark.listener.HwStepExecutionListener;
import com.roark.model.Product;
import com.roark.processor.InMemeItemProcessor;
import com.roark.reader.InMemReader;
import com.roark.writer.ConsoleItemWriter;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@EnableBatchProcessing
@Configuration
public class BatchCondifguration {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private HwJobExecutionListener hwJobExecutionListener;

    @Autowired
    private HwStepExecutionListener hwStepExecutionListener;
    
    @Autowired
    private InMemeItemProcessor inMemeItemProcessor;


    public Tasklet helloWorldTasklet(){
        return (new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello world  " );
                return RepeatStatus.FINISHED;
            }
        });
    }

    @Bean
    public Step step1(){
        return steps.get("step1")
                .listener(hwStepExecutionListener)
                .tasklet(helloWorldTasklet())
                .build();
    }

    @Bean
    public InMemReader reader(){
       return new InMemReader();
    }


    @Bean
    public Step step2(){
        return steps.get("step2").
                <Integer,Integer>chunk(3)
                .reader(flatFileItemReader(null))
            //    .reader(reader())
               // .processor(inMemeItemProcessor)
                .writer(new ConsoleItemWriter())
                .build();
    }

    @Bean
    public Job helloWorldJob(){
        return jobs.get("helloWorldJob")
        		.incrementer(new RunIdIncrementer())
                .listener(hwJobExecutionListener)
                .start(step1())
                .next(step2())
                .build();
    }
    
    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(@Value( "#{jobParameters['inputFile']}" ) FileSystemResource inputFile){
    //	@Value( "#{jobParameters['fileInput']}" ) FileSystemResource inputFile 
        FlatFileItemReader<Product> reader = new FlatFileItemReader<Product>();
        // step 1 let reader know where is the file
        // inputFile = input/product.csv
      //  reader.setResource( new  FileSystemResource("input/product.csv"));
        reader.setResource(inputFile);


        //create the line Mapper
        reader.setLineMapper(
                new DefaultLineMapper<Product>(){
                    {
                        setLineTokenizer( new DelimitedLineTokenizer() {
                            {
                                setNames( new String[]{"prodId","productName","prodDesc","price","unit"});
                                setDelimiter("|");
                            }
                        });

                        setFieldSetMapper( new BeanWrapperFieldSetMapper<Product>(){
                            {
                                setTargetType(Product.class);
                            }
                        });
                    }
                }

        );
        //step 3 tell reader to skip the header
        reader.setLinesToSkip(1);
        return reader;

    }


}
