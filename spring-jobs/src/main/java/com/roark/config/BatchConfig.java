package com.roark.config;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.roark.model.Product;

@EnableBatchProcessing
@Configuration
public class BatchConfig {
	
	//type=xml inputFile=input/product.csv fileOutput=output/product_out.csv

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private JobBuilderFactory jobs;

    @Bean
    @StepScope
    public FlatFileItemReader reader(
            @Value( "#{jobParameters['inputFile']}"  ) FileSystemResource inputFile
            ){

        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(inputFile);
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper(){
            {
                setFieldSetMapper(new BeanWrapperFieldSetMapper(){
                    {
                        setTargetType(Product.class);
                    }
                });

                setLineTokenizer(new DelimitedLineTokenizer(){
                    {
                        setNames(new String[]{"productId","prodName","productDesc"  ,"price","unit"});
                        setDelimiter(",");
                    }
                });
            }
        });

        return reader;
    }

   @Bean
   @StepScope
   public FlatFileItemWriter flatFileItemWriter(
           @Value("#{jobParameters['fileOutput']}" )FileSystemResource outputFile
     ){
        FlatFileItemWriter writer = new FlatFileItemWriter();

        writer.setResource(outputFile);
        writer.setLineAggregator( new DelimitedLineAggregator(){
            {
               
                setFieldExtractor(new BeanWrapperFieldExtractor(){
                    {
                        setNames(new String[]{"productId","prodName","productDesc","price","unit" });
                    }
                });
                setDelimiter("|");
            }
        });

        // how to write the header
        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("productID,productName,ProductDesc,price,unit");
            }
        });

        writer.setAppendAllowed(false);

        writer.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write(" The file is created at " + new SimpleDateFormat().format(new Date()));
            }
        });
        return writer;
   }
   
   @Bean
   @StepScope
  public StaxEventItemWriter xmlWriter( @Value("#{jobParameters['fileOutput']}" )FileSystemResource outputFile){

       XStreamMarshaller marshaller = new XStreamMarshaller();
       HashMap<String,Class> aliases = new HashMap<>();
       aliases.put("product",Product.class);
       marshaller.setAliases(aliases);
       marshaller.setAutodetectAnnotations(true);

       
       StaxEventItemWriter staxEventItemWriter = new StaxEventItemWriter();

       staxEventItemWriter.setResource(outputFile);
       staxEventItemWriter.setMarshaller(marshaller);
       staxEventItemWriter.setRootTagName("Products");

       return staxEventItemWriter;


  }




   @Bean
   public Step step1(){
        return steps.get("step1")
                .<Product,Product>chunk(3)
                .reader(reader(null))
               // .writer(flatFileItemWriter(null))
                .writer(xmlWriter(null))
                .build();
   }

   @Bean
   public Job job1(){
        return jobs.get("job1")
        		.incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
   }

}
