package com.sme.kafka.plain.ksqldb;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.ConfigLoader;
import com.sme.kafka.plain.admin.AdminTopic;
import com.sme.kafka.plain.model.Config;
import com.sme.kafka.plain.util.PropertiesBuilder;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;

/**
 * Unit tests to work with table operations.
 */
public class TableOperationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableOperationTest.class);

    private static Config CONFIG = new ConfigLoader().load();
    private static AdminTopic ADMIN_TOPIC = new AdminTopic(CONFIG);

    private static String UNIQUE_KEY_VALUE_TOPIC_1 = "UNIQUE_KEY_VALUE_TOPIC_1";
    private static String UNIQUE_KEY_VALUE_TOPIC_2 = "UNIQUE_KEY_VALUE_TOPIC_2";

    private Producer<Integer, String> producer;
    private Client client;

    @BeforeAll
    public static void beforeAll()
    {
        ADMIN_TOPIC.removeTopics(asList(UNIQUE_KEY_VALUE_TOPIC_1, UNIQUE_KEY_VALUE_TOPIC_2));

        assertTrue(ADMIN_TOPIC.createTopics(asList(UNIQUE_KEY_VALUE_TOPIC_1, UNIQUE_KEY_VALUE_TOPIC_2)), "Expects created topics properly");
        LOGGER.debug("All created {} topics in the cluster", ADMIN_TOPIC.list());
    }

    @AfterAll
    public static void afterAll()
    {
    }

    @BeforeEach
    public void setUp()
    {
        ClientOptions options = ClientOptions.create()
                .setHost(CONFIG.getkSqlDbServerHost())
                .setPort(CONFIG.getkSqlDbServerPort());

        client = Client.create(options);

        producer = new KafkaProducer<>(new PropertiesBuilder<String, String>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CONFIG.getHost())
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .build());

        // create data in UNIQUE_KEY_VALUE_TOPIC_1 topic
        IntStream.range(0, 100)
                .forEach(step -> producer.send(new ProducerRecord<>(UNIQUE_KEY_VALUE_TOPIC_1, step, "UNIQUE_KEY_VALUE_TOPIC_1: step = " + step)));

        // create data in UNIQUE_KEY_VALUE_TOPIC_2 topic
        IntStream.range(0, 20)
                .forEach(step -> producer.send(new ProducerRecord<>(UNIQUE_KEY_VALUE_TOPIC_2, step, "UNIQUE_KEY_VALUE_TOPIC_2: step = " + step)));

        producer.flush();
    }

    @AfterEach
    public void tearDown()
    {
    }

    @Test
    void testTableOperations() throws Exception
    {
        ExecuteStatementResult dropTablesResult1 = client.executeStatement("DROP TABLE IF EXISTS KEY_VALUE_TOPIC_1;").get();
        ExecuteStatementResult dropTablesResult2 = client.executeStatement("DROP TABLE IF EXISTS KEY_VALUE_TOPIC_2;").get();

        final String createTable1 = "CREATE TABLE KEY_VALUE_TOPIC_1 (" +
            "     id BIGINT," +
            "     value VARCHAR," +
            "   ) WITH (" +
            "     KAFKA_TOPIC = 'UNIQUE_KEY_VALUE_TOPIC_1', " +
            "     VALUE_FORMAT = 'JSON'" +
            "   );";

        final String createTable2 = "CREATE TABLE KEY_VALUE_TOPIC_2 (" +
            "     id BIGINT," +
            "     value VARCHAR," +
            "   ) WITH (" +
            "     KAFKA_TOPIC = 'UNIQUE_KEY_VALUE_TOPIC_2', " +
            "     VALUE_FORMAT = 'JSON'" +
            "   );";

        ExecuteStatementResult createTable1Result = client.executeStatement(createTable1).get();
        ExecuteStatementResult createTable2Result = client.executeStatement(createTable2).get();

        StreamedQueryResult streamedQueryResult1 = client.streamQuery("SELECT * FROM KEY_VALUE_TOPIC_1 EMIT CHANGES;").get();

        for (int i = 0; i < 10; i++)
        {
            // Block until a new row is available
            Row row = streamedQueryResult1.poll();
            if (row != null)
            {
                System.out.println("Received a row!");
                System.out.println("Row: " + row.values());
            }
            else
            {
                System.out.println("Query has ended.");
            }
        }

    }

}
