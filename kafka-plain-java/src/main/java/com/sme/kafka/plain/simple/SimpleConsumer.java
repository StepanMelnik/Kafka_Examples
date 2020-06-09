package com.sme.kafka.plain.simple;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.model.Config;

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
    private static final int ONE_THOUSAND = 1000;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private KafkaConsumer<String, String> kafkaConsumer;

    SimpleConsumer(Config config)
    {
        init(config);
    }

    private void init(Config config)
    {
        Properties configProperties = new Properties();
        configProperties.put(BOOTSTRAP_SERVERS_CONFIG, config.getHost());
        configProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        configProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(GROUP_ID_CONFIG, "1");
        configProperties.put(CLIENT_ID_CONFIG, "simple");
        configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.subscribe(singletonList(config.getTopic()));
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
            ConsumerRecords<String, String> records = kafkaConsumer.poll(ofMillis(ONE_THOUSAND));

            // Get the assigned partitions
            Set<TopicPartition> assignedPartitions = kafkaConsumer.assignment();

            // Seek to the end of those partitions
            kafkaConsumer.seekToEnd(assignedPartitions);

            Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignedPartitions);
            LOGGER.debug("End offsets " + endOffsets);

            // Now get the last message
            records = kafkaConsumer.poll(ofMillis(ONE_THOUSAND));

            ConsumerRecord<String, String> record = StreamSupport.stream(Spliterators.spliteratorUnknownSize(records.iterator(), Spliterator.ORDERED), false)
                    .skip(records.count() - 1)  // skip all except last record
                    .findFirst()
                    .orElseThrow(() -> new IllegalAccessException("The queue is empty"));

            LOGGER.debug("Record Key " + record.key());
            LOGGER.debug("Record Value " + record.value());
            LOGGER.debug("Record partition " + record.partition());
            LOGGER.debug("Record offset " + record.offset());
            return record.value();
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
