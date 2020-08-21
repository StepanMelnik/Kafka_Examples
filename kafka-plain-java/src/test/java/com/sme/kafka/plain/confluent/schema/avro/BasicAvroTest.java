package com.sme.kafka.plain.confluent.schema.avro;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.ConfigLoader;
import com.sme.kafka.plain.admin.AdminTopic;
import com.sme.kafka.plain.model.Config;
import com.sme.kafka.plain.util.PropertiesBuilder;
import com.sme.kafka.plain.util.ThreadUtil;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

/**
 * Basic avro schema test.
 */
public class BasicAvroTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicAvroTest.class);

    private static final String PAGE_VIEW_TOPIC = "PageViews";
    private static final String USER_PROFILES_TOPIC = "UserProfiles";
    private static final String RESULT_TOPIC = "PageViewsByRegion";

    private Config config;
    private AdminTopic adminTopic;
    private KafkaStreams streams;

    private Producer<String, GenericRecord> producer;
    private KafkaConsumer<String, Long> consumer;

    private static final String[] USERS = {"erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};
    private static final String[] REGIONS = {"europe", "usa", "asia", "africa"};

    private static final int STEPS = 5;

    @BeforeEach
    public void setUp()
    {
        config = new ConfigLoader().load();
        adminTopic = new AdminTopic(config);

        Topology topology = createStream();

        streams = new KafkaStreams(topology,
                new PropertiesBuilder<String, Object>()
                        .put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-avro-example")
                        .put(StreamsConfig.CLIENT_ID_CONFIG, "basic-avro-example-client")
                        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                        .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryUrl())
                        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
                        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class)
                        .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryUrl())
                        // Records should be flushed every 10 seconds. This is less than the default in order to keep this example interactive
                        .put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
                        // For illustrative purposes we disable record caches
                        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
                        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .build());

        streams.cleanUp();
        streams.start();

        producer = new KafkaProducer<>(new PropertiesBuilder<String, Object>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.RETRIES_CONFIG, 0)
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class)
                .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryUrl())
                .build());

        consumer = new KafkaConsumer<>(new PropertiesBuilder<String, Object>()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "basic-avro-group")
                .put(CLIENT_ID_CONFIG, "basic-avro-client")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
                .put("schema.registry.url", config.getSchemaRegistryUrl())
                .build());
    }

    @AfterEach
    void tearDown()
    {
        streams.close();
    }

    private Topology createStream()
    {
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, GenericRecord> views = builder.stream(PAGE_VIEW_TOPIC);

        // Create a keyed stream of page view events from the PageViews stream, by extracting the user id (String) from the Avro value.
        final KStream<String, GenericRecord> viewsByUser = views.map((dummy, record) ->
        {
            LOGGER.debug("Create a keyed steam with record: " + record.get("user").toString());
            return new KeyValue<>(record.get("user").toString(), record);
        });

        // Create a changelog stream for user profiles from the UserProfiles topic,
        // where the key of a record is assumed to be the user id (String) and its value
        // an Avro GenericRecord.  See `userprofile.avsc` under `src/main/avro/` for the
        // corresponding Avro schema.
        final KTable<String, GenericRecord> userProfiles = builder.table(USER_PROFILES_TOPIC);

        // Create a changelog stream as a projection of the value to the region attribute only
        final KTable<String, String> userRegions = userProfiles.mapValues((ValueMapper<GenericRecord, String>) record ->
        {
            LOGGER.debug("Get {} data from {} table", record.get("region").toString(), USER_PROFILES_TOPIC);
            return record.get("region").toString();
        });

        final Schema schema = loadSchema("pageviewregion.avsc");

        final KTable<Windowed<String>, Long> viewsByRegion = viewsByUser
                .leftJoin(userRegions, (view, region) ->
                {
                    final GenericRecord viewRegion = new GenericData.Record(schema);
                    viewRegion.put("user", view.get("user"));
                    viewRegion.put("page", view.get("page"));
                    viewRegion.put("region", region);
                    return viewRegion;
                })
                .map((user, viewRegion) ->
                {
                    return new KeyValue<>(viewRegion.get("region").toString(), viewRegion);
                })
                // count views by user, using hopping windows of size 5 minutes that advance every 1 minute
                .groupByKey() // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
                .count();

        final KStream<String, Long> viewsByRegionForConsole = viewsByRegion
                // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
                // and by also converting the record key from type `Windowed<String>` (which
                // kafka-console-consumer can't print to console out-of-the-box) to `String`
                .toStream((windowedRegion, count) -> windowedRegion.toString());

        // write to the result topic
        viewsByRegionForConsole.to(RESULT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

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
        assertTrue(adminTopic.createTopics(asList(PAGE_VIEW_TOPIC, USER_PROFILES_TOPIC, RESULT_TOPIC)), "Expects created topics properly");
        LOGGER.debug("All created {} topics in the cluster", adminTopic.list());

        consumer.subscribe(singletonList(RESULT_TOPIC));

        CompletableFuture<List<Record>> producerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<Record> messages = new ArrayList<>();

            final GenericRecordBuilder pageViewBuilder = new GenericRecordBuilder(loadSchema("pageview.avsc"));
            final GenericRecordBuilder userProfileBuilder = new GenericRecordBuilder(loadSchema("userprofile.avsc"));

            final Random random = new Random();
            pageViewBuilder.set("industry", "eng");

            for (final String user : USERS)
            {
                userProfileBuilder.set("experience", "Randon experience");
                userProfileBuilder.set("region", REGIONS[random.nextInt(REGIONS.length)]);
                Record userProfileRecord = userProfileBuilder.build();
                messages.add(userProfileRecord);

                producer.send(new ProducerRecord<>(USER_PROFILES_TOPIC, user, userProfileRecord));

                // For each user generate some page views
                for (int i = 0; i < random.nextInt(10); i++)
                {
                    pageViewBuilder.set("user", user);
                    pageViewBuilder.set("page", "index.html");
                    final Record record = pageViewBuilder.build();
                    messages.add(record);
                    producer.send(new ProducerRecord<>(PAGE_VIEW_TOPIC, null, record));
                }
            }

            return messages;
        });

        CompletableFuture<List<KeyValue<String, Long>>> consumerCompletableFuture = CompletableFuture.supplyAsync(() ->
        {
            List<KeyValue<String, Long>> result = new ArrayList<>();

            while (true)
            {
                LOGGER.debug("Process consumer ...");
                ThreadUtil.sleepInSeconds(2, s -> LOGGER.error(s, Thread.currentThread().getName()), "{} thread is interrupted");

                final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(3_000));
                for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords)
                {
                    LOGGER.debug("Fetch a record with \"{}\" key and \"{}\" value in the \"{}\" record", consumerRecord.key(), consumerRecord.value(), consumerRecord);
                    result.add(new KeyValue<>(consumerRecord.key(), consumerRecord.value()));
                }

                if (result.size() >= STEPS)
                {
                    LOGGER.debug("Consumer completed");
                    break;
                }
            }

            return result;
        });

        List<Record> producerMessages = producerCompletableFuture.get();
        producerMessages.forEach(action -> LOGGER.debug("Producer message: " + action));

        List<KeyValue<String, Long>> consumerMessages = consumerCompletableFuture.get(1, TimeUnit.MINUTES);
        assertTrue(consumerMessages.size() > 0, "Expects consumed messages");
        consumerMessages.forEach(action -> LOGGER.debug("Consumer message: " + action));
    }

    private static Schema loadSchema(final String name)
    {
        try (final InputStream input = BasicAvroTest.class.getClassLoader().getResourceAsStream("com/sme/kafka/plain/confluent/schema/avro/" + name))
        {
            return new Schema.Parser().parse(input);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot load schema: " + name);
        }
    }
}
