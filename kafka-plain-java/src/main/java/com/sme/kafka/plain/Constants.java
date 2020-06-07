package com.sme.kafka.plain;

/**
 * Constants to work with Kafka container.
 */
public final class Constants
{
    public static final String KAFKA_HOST = "192.168.0.109:9092";

    /**
     * The topic name to test by unit tests.
     * 
     * <pre>
     * {@code
     *     > bin/kafka-topics.sh --create --bootstrap-server 192.168.0.109:9092 --replication-factor 1 --partitions 1 --topic Hello
     *     > > bin/kafka-topics.sh --list --bootstrap-server 192.168.0.109:9092
     * }
     * </pre>
     */
    public static final String HELLO_TOPIC_NAME = "HelloKafka";

    private Constants()
    {
    }
}
