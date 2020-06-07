package com.sme.kafka.plain.stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import com.sme.kafka.plain.Constants;

/**
 * Kafka Stream builder allows to transform a messge.
 */
public class Stream
{
    private KafkaStreams streams;

    public Stream()
    {
        init();
    }

    private void init()
    {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-example");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);

        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String> stream(Constants.HELLO_TOPIC_NAME)
                .mapValues(value -> value.toUpperCase())
                .to(Constants.HELLO_TOPIC_NAME);

        streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
    }

    /**
     * Stop all streams.
     */
    public void stop()
    {
        streams.close();
    }
}
