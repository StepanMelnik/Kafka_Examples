package com.sme.kafka.plain.ksqldb;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.ConfigLoader;
import com.sme.kafka.plain.model.Config;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;

/**
 * Test a simple connection.
 */
public class SimpleClientConnectionTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClientConnectionTest.class);

    /**
     * http://192.168.0.109:8088/info <-- ksql db server
     */
    @Test
    void testListObjects() throws Exception
    {
        Config config = new ConfigLoader().load();

        ClientOptions options = ClientOptions.create()
                .setHost(config.getkSqlDbServerHost())
                .setPort(config.getkSqlDbServerPort());

        Client client = Client.create(options);

        List<TopicInfo> topics = client.listTopics().get();
        topics.forEach(t -> LOGGER.debug("Fetched {} topic", t.toString()));

        List<TableInfo> tables = client.listTables().get();
        tables.forEach(t -> LOGGER.debug("Fetched {} table", t.toString()));

        List<StreamInfo> streams = client.listStreams().get();
        streams.forEach(t -> LOGGER.debug("Fetched {} stream", t.toString()));

        List<QueryInfo> queries = client.listQueries().get();
        queries.forEach(t -> LOGGER.debug("Fetched {} stream", t.toString()));

        client.close();
    }
}
