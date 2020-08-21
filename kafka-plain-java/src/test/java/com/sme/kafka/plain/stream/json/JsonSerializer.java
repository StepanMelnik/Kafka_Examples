package com.sme.kafka.plain.stream.json;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json POJO serializer.
 */
public class JsonSerializer<T> implements Serializer<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void close()
    {
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey)
    {
    }

    @Override
    public byte[] serialize(String topic, T data)
    {
        try
        {
            String writeValueAsString = objectMapper.writeValueAsString(data);
            LOGGER.debug("Serialize {} value {} class in {} topic", writeValueAsString, data.getClass(), topic);
            return writeValueAsString.getBytes();
        }
        catch (JsonProcessingException e)
        {
            LOGGER.error("Cannot serialize {} data", data);
            throw new SerializationException(e);
        }
    }
}
