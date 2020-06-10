package com.sme.kafka.spring.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Component to create a list of topics from configuration file.
 */
@Component
@PropertySource("classpath:application.yml")
@ConfigurationProperties(prefix = "kafka", ignoreUnknownFields = true)
public class KafkaTopicConfiguration
{
    private List<KafkaTopic> topics = new ArrayList<>();

    public List<KafkaTopic> getTopics()
    {
        return topics;
    }

    public void setTopics(List<KafkaTopic> topics)
    {
        this.topics = topics;
    }

    /**
     * Represents a pojo configuration in configuration file.
     */
    static class KafkaTopic
    {
        private String name;
        private int partitions;
        private short replicationFactor;

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public int getPartitions()
        {
            return partitions;
        }

        public void setPartitions(int partitions)
        {
            this.partitions = partitions;
        }

        public short getReplicationFactor()
        {
            return replicationFactor;
        }

        public void setReplicationFactor(short replicationFactor)
        {
            this.replicationFactor = replicationFactor;
        }
    }
}
