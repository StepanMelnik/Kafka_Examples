package com.sme.kafka.plain.simple;

import static com.sme.kafka.plain.Constants.HELLO_TOPIC_NAME;
import static com.sme.kafka.plain.Constants.KAFKA_HOST;
import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Kafka consumer.
 * 
 * <pre>
 * {@code
 *     > bin/kafka-console-consumer.sh --bootstrap-server 192.168.0.109:9092 --topic Hello --from-beginning
 * }
 * </pre>
 */
class SimpleConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);
    private static final int ONE_HUNDRED = 100;
    private static final int ONE_THOUSAND = 1000;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private KafkaConsumer<String, String> kafkaConsumer;

    SimpleConsumer()
    {
        init();
    }

    private void init()
    {
        Properties configProperties = new Properties();
        configProperties.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        configProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        configProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(GROUP_ID_CONFIG, "1");
        configProperties.put(CLIENT_ID_CONFIG, "simple");

        kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.subscribe(singletonList(HELLO_TOPIC_NAME));
    }

    /**
     * Fetch a message from Kafka by consumer.
     * 
     * @return Returns a message from consumer.
     */
    String getMessage()
    {
        Callable<String> task = () ->
        {
            int noMessageFound = 0;

            while (true)
            {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(ofMillis(ONE_THOUSAND));

                // FIXME crazy code
                if (records.isEmpty())
                {
                    noMessageFound++;
                    if (noMessageFound > ONE_HUNDRED)
                    {
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }

                ConsumerRecord<String, String> record = stream(spliteratorUnknownSize(records.iterator(), ORDERED), false)
                        .findFirst()
                        .orElseThrow(() -> new IllegalAccessException("The queue is empty"));

                LOGGER.debug("Record Key " + record.key());
                LOGGER.debug("Record Value " + record.value());
                LOGGER.debug("Record partition " + record.partition());
                LOGGER.debug("Record offset " + record.offset());
                return record.value();
            }

            return "NO_MESSAGE";
        };

        try
        {
            return executor.submit(task).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException("Cannot fetch a message", e);
        }
    }

    void stop()
    {
        kafkaConsumer.close();
        executor.shutdown();
    }
}
