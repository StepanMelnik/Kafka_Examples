package com.sme.kafka.plain.stream.json;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json POJO deserializer.
 */
public class JsonDeserializer<T> implements Deserializer<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonDeserializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> type;

    /*
     * Default constructor needed by kafka to create deserizlizer in consumer. The type is configured in {@link this#configure}
     */
    public JsonDeserializer()
    {
    }

    public JsonDeserializer(Class<T> type)
    {
        this.type = type;
    }

    @Override
    public void close()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> map, boolean arg1)
    {
        if (type == null)
        {
            type = (Class<T>) map.get("type");
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes)
    {
        if (bytes == null || bytes.length == 0)
        {
            return null;
        }

        try
        {
            T data = objectMapper.readValue(bytes, type);
            LOGGER.debug("Desrialized {} type with {} value", type, data);
            return data;
        }
        catch (Exception e)
        {
            LOGGER.error("Cannot deserialize {} type with {} bytes", type, bytes);
            throw new SerializationException(e);
        }
    }
}
