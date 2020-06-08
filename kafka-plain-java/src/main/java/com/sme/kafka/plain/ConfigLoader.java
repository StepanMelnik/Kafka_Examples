package com.sme.kafka.plain;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sme.kafka.plain.model.Config;

/**
 * Parses config.json and prepares {@link Config} instance.
 */
public class ConfigLoader
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses config.json and prepares {@link Config} instance.
     * 
     * @return Returns {@link Config} instance.
     */
    public Config load()
    {
        InputStream stream = ConfigLoader.class.getClassLoader().getResourceAsStream("config.json");

        try
        {
            return MAPPER.readValue(stream, new TypeReference<Config>()
            {
            });
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Cannot read and parse config.json file", e);
        }
    }
}
