package com.sme.kafka.plain.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.sme.kafka.plain.ConfigLoader;
import com.sme.kafka.plain.admin.AdminTopic;
import com.sme.kafka.plain.model.Config;
import com.sme.kafka.plain.util.PropertiesBuilder;

/**
 * Abstraction to work with Kafka streams unit tests.
 */
public abstract class AStreamTest
{
    protected Config config;
    protected AdminTopic adminTopic;
    protected KafkaStreams streams;

    @BeforeEach
    public void setUp()
    {
        config = new ConfigLoader().load();
        adminTopic = new AdminTopic(config);

        //assertTrue(adminTopic.removeAllTopics(), "Expects removed topics properly");

        Topology topology = createStream();

        streams = new KafkaStreams(topology,
                new PropertiesBuilder<String, Object>()
                        .put(StreamsConfig.APPLICATION_ID_CONFIG, getStreamApplicationIdConfig())
                        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost())
                        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
                        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
                        // Records should be flushed every 10 seconds. This is less than the default in order to keep this example interactive
                        .put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
                        // For illustrative purposes we disable record caches
                        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
                        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .build());

        streams.cleanUp();
        streams.start();
    }

    /**
     * Get application id config name.
     * 
     * @return Returns the application id config.
     */
    protected abstract String getStreamApplicationIdConfig();

    @AfterEach
    void tearDown()
    {
        streams.close();
    }

    /**
     * Create {@link StreamsBuilder} instance.
     * 
     * @return Returns a stream builder.
     */
    protected abstract Topology createStream();
}
