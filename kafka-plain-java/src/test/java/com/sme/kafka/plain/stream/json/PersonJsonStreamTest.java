package com.sme.kafka.plain.stream.json;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.stream.AStreamTest;
import com.sme.kafka.plain.util.PropertiesBuilder;
import com.sme.kafka.plain.util.ThreadUtil;

/**
 * Test Kafka streams to work with json stream.
 */
public class PersonJsonStreamTest extends AStreamTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonJsonStreamTest.class);

    private static final String SOURCE_TOPIC = "JsonPersonSource";
    private static final String TARGET_TOPIC = "JsonPersonTarget";

    private static final int STEPS = 10;

    private Producer<String, Person> producer;
    private KafkaConsumer<String, Person> consumer;

    @BeforeEach
    @Override
    public void setUp()
    {
        super.setUp();

        producer = new KafkaProducer<>(new PropertiesBuilder<String, Object>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class)
                .build());

        consumer = new KafkaConsumer<>(new PropertiesBuilder<String, Object>()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.sme.kafka.plain.stream.json.JsonDeserializer")
                .put("type", Person.class)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "person-json-group")
                .put(CLIENT_ID_CONFIG, "person-json-client")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build());
    }

    @Override
    protected String getStreamApplicationIdConfig()
    {
        return "stream-person-json-example";
    }

    @Override
    protected Topology createStream()
    {
        Serde<Person> personSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Person.class));

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Person> kStream = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), personSerde));

        kStream.foreach((key, person) ->
        {
            LOGGER.debug("Process {} key and {} person", key, person);
        });

        kStream.to(TARGET_TOPIC);
        return builder.build();
    }

    /**
     * <pre>
     * Test Plan:
     * 1) start kafka:
     *      * sudo bin/zookeeper-server-start.sh config/zookeeper.properties
     *      * sudo bin/kafka-server-start.sh config/server.properties
     * 2) remove topics
     * 3) create topics:
     *      * by java api: @link {AdminTopic#createTopics}
     *      * or by shell: bin/kafka-topics.sh --create --topic TOPIC --zookeeper localhost:2181 --partitions 1 --replication-factor 1
     * 4) fetch all used topics:
     *      * by java api: @link {AdminTopic#list}
     *      * or by shell:  sudo bin/kafka-topics.sh --list --zookeeper localhost:2181
     * 5) start unit test:
     *      * send messages by producer
     *      * consume messages
     * 6) assert a result
     * 
     * Debug:
     * sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic PersonJsonSource
     *  > enter text
     * 
     * check that data is sent to source consumer:  
     * sudo bin/kafka-console-consumer.sh --topic JsonPersonSource --from-beginning --bootstrap-server localhost:9092
     * check that data is processed in target consumer:
     * sudo bin/kafka-console-consumer.sh --topic JsonPersonTarget --from-beginning --bootstrap-server localhost:9092
     * </pre>
     */
    @Test
    void testProccess() throws Exception
    {
        //assertTrue(adminTopic.removeTopics(asList(SOURCE_TOPIC, TARGET_TOPIC)), "Expects removed topics properly");
        assertTrue(adminTopic.createTopics(asList(SOURCE_TOPIC, TARGET_TOPIC)), "Expects created topics properly");
        LOGGER.debug("All created {} topics in the cluster", adminTopic.list());

        //consumer.subscribe(singletonList(TARGET_TOPIC));
        consumer.subscribe(asList(SOURCE_TOPIC, TARGET_TOPIC));

        CompletableFuture<List<Person>> producerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<Person> messages = new ArrayList<>();

            IntStream.range(0, STEPS)
                    .forEach(step ->
                    {
                        ThreadUtil.sleepInSeconds(1, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");
                        LOGGER.debug("Process producer in {} step", step);

                        Person person = new Person(String.valueOf(step), "name" + step, "country" + step, "occupation" + step);
                        messages.add(person);
                        LOGGER.debug("Send \"{}\" value", person);

                        ProducerRecord<String, Person> record = new ProducerRecord<>(SOURCE_TOPIC, "test" + step, person);
                        producer.send(record);
                        producer.flush();
                    });

            return messages;
        });

        CompletableFuture<List<Person>> consumerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<Person> result = new ArrayList<>();

            while (true)
            {
                LOGGER.debug("Process consumer ...");
                ThreadUtil.sleepInSeconds(2, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");

                final ConsumerRecords<String, Person> consumerRecords = consumer.poll(Duration.ofMillis(3_000));
                for (final ConsumerRecord<String, Person> consumerRecord : consumerRecords)
                {
                    LOGGER.debug("Fetch a record with \"{}\" key and \"{}\" value in the \"{}\" record", consumerRecord.key(), consumerRecord.value(), consumerRecord);
                    result.add(consumerRecord.value());
                }

                if (result.size() >= STEPS)
                {
                    LOGGER.debug("Consumer completed");
                    break;
                }
            }

            return result;
        });

        List<Person> producerMessages = producerCompletableFuture.get();
        producerMessages.forEach(action -> LOGGER.debug("Producer message: " + action));

        List<Person> consumerMessages = consumerCompletableFuture.get(1, TimeUnit.MINUTES);
        assertTrue(consumerMessages.size() > 0, "Expects consumed messages");
        consumerMessages.forEach(action -> LOGGER.debug("Consumer message: " + action));
    }
}
