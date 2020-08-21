package com.sme.kafka.plain.stream.processor;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.model.KeyValueRow;
import com.sme.kafka.plain.stream.AStreamTest;
import com.sme.kafka.plain.util.PropertiesBuilder;
import com.sme.kafka.plain.util.ThreadUtil;

/**
 * Test Kafka streams to count words by a processor.
 */
public class WordCountProcessorTest extends AStreamTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountProcessorTest.class);

    private static final String SOURCE_TOPIC = "WordsCountProcessorSource";
    private static final String TARGET_TOPIC = "WordsCountProcessorTarget";
    private static final String STORE_NAME = "Counts";

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
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .build());

        consumer = new KafkaConsumer<>(new PropertiesBuilder<String, Object>()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "stream-word-count-processor-example_" + UUID.randomUUID().toString())
                .put(ConsumerConfig.CLIENT_ID_CONFIG, "word_counts_processor_client")
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
        return "stream-processor-word-count-example";
    }

    @Override
    protected Topology createStream()
    {
        final Topology topology = new Topology();

        topology.addSource("Source", SOURCE_TOPIC);
        topology.addProcessor("Process", new WordCountProcessorSupplier(), "Source");

        topology.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.Long()),
                "Process");

        topology.addSink("Sink", TARGET_TOPIC, "Process");

        return topology;
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

                        String value = "New record with value = " + step % 2;
                        messages.add(value);
                        LOGGER.debug("Send \"{}\" value", value);

                        ProducerRecord<String, String> record = new ProducerRecord<>(SOURCE_TOPIC, String.valueOf(step), value);
                        producer.send(record);
                        producer.flush();
                    });

            return messages;
        });

        CompletableFuture<List<KeyValueRow<String, Long>>> consumerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<KeyValueRow<String, Long>> result = new ArrayList<>();

            while (true)
            {
                LOGGER.debug("Process consumer ...");
                ThreadUtil.sleepInSeconds(1, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");

                final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(3_000));
                for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords)
                {
                    LOGGER.debug("Fetch a record with \"{}\" key and \"{}\" value in the \"{}\" record", consumerRecord.key(), consumerRecord.value(), consumerRecord);
                    result.add(new KeyValueRow<>(consumerRecord.key(), consumerRecord.value()));
                }

                if (result.size() >= STEPS / 2)
                {
                    LOGGER.debug("Consumer completed");
                    break;
                }
            }

            return result;
        });

        List<String> producerMessages = producerCompletableFuture.get();
        producerMessages.forEach(action -> LOGGER.debug("Producer message: " + action));

        List<KeyValueRow<String, Long>> consumerMessages = consumerCompletableFuture.get(1, TimeUnit.MINUTES);
        assertTrue(consumerMessages.size() > 0, "Expects consumed messages");
        consumerMessages.forEach(action -> LOGGER.debug("Consumer message: " + action));
    }

    /**
     * Provides processor to work with stream in memory store.
     */
    private static class WordCountProcessorSupplier implements ProcessorSupplier<String, String>
    {
        @Override
        public Processor<String, String> get()
        {
            return new Processor<String, String>()
            {
                private ProcessorContext context;
                private KeyValueStore<String, Long> keyValueStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context)
                {
                    this.context = context;
                    this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp ->
                    {
                        try (final KeyValueIterator<String, Long> iter = keyValueStore.all())
                        {
                            LOGGER.debug("WordCountProcessorSupplier: Process a sheduled context in {} timestamp", timestamp);

                            while (iter.hasNext())
                            {
                                final KeyValue<String, Long> entry = iter.next();
                                LOGGER.debug("WordCountProcessorSupplier: process \"{}\" key and \"{}\" value", entry.key, entry.value);
                                context.forward(entry.key, entry.value.toString());
                            }
                        }
                    });
                    this.keyValueStore = (KeyValueStore<String, Long>) context.getStateStore(STORE_NAME);
                }

                @Override
                public void process(final String dummy, final String line)
                {
                    final String[] words = line.toLowerCase(Locale.getDefault()).split(" ");

                    for (final String word : words)
                    {
                        final Long oldValue = this.keyValueStore.get(word);

                        if (oldValue == null)
                        {
                            this.keyValueStore.put(word, new Long(1));
                        }
                        else
                        {
                            this.keyValueStore.put(word, oldValue + 1);
                        }
                    }

                    context.commit();
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
