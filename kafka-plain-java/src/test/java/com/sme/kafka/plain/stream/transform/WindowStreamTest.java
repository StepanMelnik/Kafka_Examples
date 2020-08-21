package com.sme.kafka.plain.stream.transform;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.model.KeyValueRow;
import com.sme.kafka.plain.stream.AStreamTest;
import com.sme.kafka.plain.util.PropertiesBuilder;
import com.sme.kafka.plain.util.ThreadUtil;

/**
 * Test Kafka streams to work with a stream in a windowed range.
 */
public class WindowStreamTest extends AStreamTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WindowStreamTest.class);

    private static final String SOURCE_TOPIC = "WindowSource_1";
    private static final String TARGET_TOPIC = "WindowTarget_1";

    private static final int STEPS = 10;
    private static final int WINDOW_SIZE = 5;
    private static final int THRESHOLD = 5;

    private Producer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    @Override
    public void setUp()
    {
        super.setUp();

        producer = new KafkaProducer<>(new PropertiesBuilder<String, String>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .build());

        consumer = new KafkaConsumer<>(new PropertiesBuilder<String, Object>()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "stream-window-example_" + UUID.randomUUID().toString())
                .put(ConsumerConfig.CLIENT_ID_CONFIG, "window_case_client_1")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100_000)
                .put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300_000)
                .put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 300_000)
                .build(),
                new StringDeserializer(),   // word
                new StringDeserializer());    // count
    }

    @Override
    protected String getStreamApplicationIdConfig()
    {
        return "stream-window-example-1";
    }

    @Override
    protected Topology createStream()
    {
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder.stream(SOURCE_TOPIC);

        final KStream<Windowed<String>, String> max = textLines
                .selectKey((key, value) ->
                {
                    LOGGER.debug("Select new key for {} key and {} value", key, value);
                    return "windowed";
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(WINDOW_SIZE)))
                .reduce((value1, value2) ->
                {
                    if (Integer.parseInt(value1) > Integer.parseInt(value2))
                    {
                        return value1;
                    }
                    else
                    {
                        return value2;
                    }
                })
                .toStream()
                .filter((key, value) ->
                {
                    return Integer.parseInt(value) > THRESHOLD;
                });

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        // need to override key serde to Windowed<String> type
        max.to("WindowTarget", Produced.with(windowedSerde, Serdes.String()));

        return builder.build();
    }

    /**
     * <pre>
     * Test Plan:
     * 1) start kafka:
     *      * sudo bin/zookeeper-server-start.sh config/zookeeper.properties
     *      * sudo bin/kafka-server-start.sh config/server.properties
     * 2) remove all topics
     * 3) create topics:
     *      * by java api: @link {AdminTopic#createTopics}
     *      * or by shell: bin/kafka-topics.sh --create --topic TOPIC_SOURCE --zookeeper 192.168.0.109:2181 --partitions 1 --replication-factor 1
     * 4) fetch all used topics:
     *      * by java api: @link {AdminTopic#list}
     *      * or by shell:  sudo bin/kafka-topics.sh --list --zookeeper 192.168.0.109:2181
     * 5) start unit test:
     *      * send messages by producer
     *      * consume messages
     * 6) assert a result
     * </pre>
     * 
     * <pre>
     * Test producer/consumer by hand:
     * sudo bin/kafka-console-producer.sh --broker-list 192.168.0.109:9092 --topic WordsCountSource_1
     *  > enter text
     * sudo bin/kafka-console-consumer.sh --topic WordsCountTarget_1 --from-beginning --bootstrap-server 192.168.0.109:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
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

            IntStream.range(1, STEPS)
                    .forEach(step ->
                    {
                        ThreadUtil.sleepInSeconds(1, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");
                        LOGGER.debug("Process producer in {} step", step);

                        String value = String.valueOf(step);
                        messages.add(value);
                        LOGGER.debug("Send \"{}\" value", value);

                        ProducerRecord<String, String> record = new ProducerRecord<>(SOURCE_TOPIC, value, value);
                        producer.send(record);
                        producer.flush();
                    });

            return messages;
        });

        CompletableFuture<List<KeyValueRow<String, String>>> consumerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<KeyValueRow<String, String>> consumerMessages = new ArrayList<>();

            while (true)
            {
                LOGGER.debug("Process consumer ...");
                ThreadUtil.sleepInSeconds(1, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");

                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3_000));
                for (final ConsumerRecord<String, String> consumerRecord : consumerRecords)
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

        List<KeyValueRow<String, String>> consumerMessages = consumerCompletableFuture.get(1, TimeUnit.MINUTES);
        assertTrue(consumerMessages.size() > 0, "Expects consumed messages");
        consumerMessages.forEach(action -> LOGGER.debug("Consumer message: " + action));
    }
}
