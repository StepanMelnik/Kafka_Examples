package com.sme.kafka.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring boot application to work with Kafka container.
 */
@SpringBootApplication
public class KafkaSpringBootApplication
{
    /**
     * Main entry point.
     * 
     * @param args The list of arguments to start JVM.
     */
    public static void main(String[] args)
    {
        SpringApplication.run(KafkaSpringBootApplication.class, args);
    }
}
