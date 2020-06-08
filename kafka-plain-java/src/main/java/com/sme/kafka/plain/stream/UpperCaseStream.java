package com.sme.kafka.plain.stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import com.sme.kafka.plain.model.Config;

/**
 * Kafka Stream builder allows to transform a message.
 * <p>
 * The current stream transforms all messages to Upper Case.
 * </p>
 */
public class UpperCaseStream
{
    private KafkaStreams streams;

    public UpperCaseStream(Config config)
    {
        init(config);
    }

    private void init(Config config)
    {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-example");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost());

        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        // CSOFF
        builder.<String, String> stream(config.getTopic())
                .mapValues(value -> value.toUpperCase())
                .to(config.getTopic());
        // CSON

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
