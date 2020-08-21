package com.sme.kafka.plain.util;

import java.util.Properties;

/**
 * The builder to create {@link Properties} instance.
 */
public final class PropertiesBuilder<K, V>
{
    private final Properties properties = new Properties();

    /**
     * Add a row into properties.
     * 
     * @param key The key;
     * @param value The value;
     * @return Returns builder.
     */
    public PropertiesBuilder<K, V> put(K key, V value)
    {
        properties.put(key, value);
        return this;
    }

    /**
     * Return created properties.
     * 
     * @return The created properties instance.
     */
    public Properties build()
    {
        return properties;
    }
}
