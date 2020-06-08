package com.sme.kafka.plain.simple;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sme.kafka.plain.ConfigLoader;
import com.sme.kafka.plain.admin.AdminTopic;
import com.sme.kafka.plain.model.Config;
import com.sme.kafka.plain.stream.UpperCaseStream;

/**
 * Integration tests of {@link SimpeConsumer} and {@link SimpeProducer}.
 */
public class SimpleProducerConsumerTest
{
    private SimpleConsumer simpleConsumer;
    private SimpleProducer simpleProducer;
    private UpperCaseStream stream;

    @BeforeEach
    public void setUp()
    {
        Config config = new ConfigLoader().load();

        new AdminTopic(config).createTopics(asList(config.getTopic()));

        simpleConsumer = new SimpleConsumer(config);
        simpleProducer = new SimpleProducer(config);
        stream = new UpperCaseStream(config);
    }

    @AfterEach
    public void tearDown()
    {
        simpleConsumer.stop();
        simpleProducer.stop();
        stream.stop();
    }

    @Test
    void testSendAndGetMessage() throws Exception
    {
        final String message = "Hello Kafka!";
        simpleProducer.send(message);
        String actualMessage = simpleConsumer.getMessage();
        assertEquals(message.toUpperCase(), actualMessage);
    }
}
