package com.sme.kafka.plain.simple;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.sme.kafka.plain.model.Config;

/**
 * A simple Kafka producer.
 * 
 * <pre>
 * {@code
 *     > bin/kafka-console-producer.sh --bootstrap-server 192.168.0.109:9092 --topic Hello
 * }
 * </pre>
 */
class SimpleProducer
{
    private Producer<String, String> producer;
    private final Config config;

    SimpleProducer(Config config)
    {
        this.config = config;
        init();
    }

    private void init()
    {
        Properties configProperties = new Properties();
        configProperties.put(BOOTSTRAP_SERVERS_CONFIG, config.getHost());
        configProperties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(configProperties);
    }

    /**
     * Send a message.
     * 
     * @param message The given message to send to Kafka.
     */
    void send(String message)
    {
        ProducerRecord<String, String> rec = new ProducerRecord<>(config.getTopic(), message);
        producer.send(rec);
    }

    /**
     * Close Kafka producer.
     */
    void stop()
    {
        producer.close();
    }
}
