package com.sme.kafka.plain.stream.transform;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.model.KeyValueRow;
import com.sme.kafka.plain.stream.AStreamTest;
import com.sme.kafka.plain.util.PropertiesBuilder;
import com.sme.kafka.plain.util.ThreadUtil;

/**
 * Test Kafka streams to count words.
 */
public class WordCountStreamTest extends AStreamTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStreamTest.class);

    private static final String SOURCE_TOPIC = "WordsCountSource_1";
    private static final String TARGET_TOPIC = "WordsCountTarget_1";

    private static final int STEPS = 10;

    private Producer<String, String> producer;
    private KafkaConsumer<String, Long> consumer;

    @BeforeEach
    @Override
    public void setUp()
    {
        super.setUp();

        producer = new KafkaProducer<>(new PropertiesBuilder<String, String>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .build());

        consumer = new KafkaConsumer<>(new PropertiesBuilder<String, Object>()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "stream-words-count-example_" + UUID.randomUUID().toString())
                //.put(ConsumerConfig.GROUP_ID_CONFIG, "stream-words-count-example")
                .put(ConsumerConfig.CLIENT_ID_CONFIG, "counts_word_case_client")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100_000)
                .put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300_000)
                .put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 300_000)
                .build(),
                new StringDeserializer(),   // word
                new LongDeserializer());    // count
    }

    @Override
    protected String getStreamApplicationIdConfig()
    {
        return "stream-words-count-example";
    }

    @Override
    protected Topology createStream()
    {
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder.stream(SOURCE_TOPIC);

        final KStream<String, String> words = textLines.flatMapValues(line -> Arrays.asList(line.split(" ")));
        words.print(Printed.toSysOut());

        final KTable<String, Long> wordCounts = words.groupBy((key, value) -> value)
                .count();

        wordCounts.toStream().to(TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    /**
     * <pre>
     * Test Plan:
     * 1) start kafka:
     *      * sudo bin/zookeeper-server-start.sh config/zookeeper.properties
     *      * sudo bin/kafka-server-start.sh config/server.properties
     * 2) create topics:
     *      * by java api: @link {AdminTopic#createTopics}
     *      * or by shell: bin/kafka-topics.sh --create --topic TOPIC --zookeeper localhost:2181 --partitions 1 --replication-factor 1
     * 3) fetch all used topics:
     *      * by java api: @link {AdminTopic#list}
     *      * or by shell:  sudo bin/kafka-topics.sh --list --zookeeper localhost:2181
     * 4) start unit test:
     *      * send messages by producer
     *      * consume messages
     * 5) assert a result
     * 
     * Debug:
     * sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic SOURCE_TOPIC
     *  > enter text
     * 
     * check that data is sent to source consumer:  
     * sudo bin/kafka-console-consumer.sh --topic SOURCE_TOPIC --from-beginning --bootstrap-server localhost:9092
     * check that data is processed in target consumer:
     * sudo bin/kafka-console-consumer.sh --topic TARGET_TOPIC --from-beginning --bootstrap-server localhost:9092
     * </pre>
     */
    @Test
    void testProccess() throws Exception
    {
        //assertTrue(adminTopic.removeTopics(asList(TOPIC_SOURCE, TOPIC_TARGET)), "Expects removed topics properly");
        LOGGER.debug("All created {} topics in the cluster", adminTopic.list());
        assertTrue(adminTopic.createTopics(asList(SOURCE_TOPIC, TARGET_TOPIC)), "Expects created topics properly");
        LOGGER.debug("All created {} topics in the cluster", adminTopic.list());

        consumer.subscribe(singletonList(TARGET_TOPIC));

        CompletableFuture<List<String>> producerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<String> messages = new ArrayList<>();

            IntStream.range(0, STEPS)
                    .forEach(step ->
                    {
                        ThreadUtil.sleepInSeconds(1, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");
                        LOGGER.debug("Process producer in {} step", step);

                        String value = String.format("New record with %d value", step % 2);
                        messages.add(value);
                        LOGGER.debug("Send \"{}\" value", value);

                        ProducerRecord<String, String> record = new ProducerRecord<>(SOURCE_TOPIC, value);
                        producer.send(record);
                        producer.flush();
                    });

            return messages;
        });

        CompletableFuture<List<KeyValueRow<String, Long>>> consumerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<KeyValueRow<String, Long>> consumerMessages = new ArrayList<>();

            while (true)
            {
                LOGGER.debug("Process consumer ...");
                ThreadUtil.sleepInSeconds(1, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");

                final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(3_000));
                for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords)
                {
                    LOGGER.debug("Fetch a record with \"{}\" key and \"{}\" value in the \"{}\" record", consumerRecord.key(), consumerRecord.value(), consumerRecord);
                    consumerMessages.add(new KeyValueRow<>(consumerRecord.key(), consumerRecord.value()));
                }

                if (consumerMessages.size() >= STEPS / 2)
                {
                    LOGGER.debug("Consumer completed");
                    break;
                }
            }
            return consumerMessages;
        });

        List<String> producerMessages = producerCompletableFuture.get();
        producerMessages.forEach(action -> LOGGER.debug("Producer message: " + action));

        List<KeyValueRow<String, Long>> consumerMessages = consumerCompletableFuture.get(1, TimeUnit.MINUTES);
        assertTrue(consumerMessages.size() > 0, "Expects consumed messages");
        consumerMessages.forEach(action -> LOGGER.debug("Consumer message: " + action));
    }
}
