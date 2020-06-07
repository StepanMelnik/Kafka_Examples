package com.sme.kafka.plain.simple;

import static com.sme.kafka.plain.Constants.HELLO_TOPIC_NAME;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sme.kafka.plain.admin.AdminTopic;
import com.sme.kafka.plain.stream.Stream;

/**
 * Integration tests of {@link SimpeConsumer} and {@link SimpeProducer}.
 */
public class SimpleProducerConsumerTest
{
    private SimpleConsumer simpleConsumer;
    private SimpleProducer simpleProducer;
    private Stream stream;

    @BeforeEach
    public void setUp()
    {
        new AdminTopic().createTopics(asList(HELLO_TOPIC_NAME));

        simpleConsumer = new SimpleConsumer();
        simpleProducer = new SimpleProducer();
        stream = new Stream();
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
