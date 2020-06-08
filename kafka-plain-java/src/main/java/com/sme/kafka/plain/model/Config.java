package com.sme.kafka.plain.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Provides configuration properties.
 */
public class Config
{
    private String host;

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
    private String topic;

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }

        Config that = (Config) obj;
        return EqualsBuilder.reflectionEquals(that, this);
    }

    @Override
    public String toString()
    {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SIMPLE_STYLE);
    }
}
