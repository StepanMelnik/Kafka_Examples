package com.sme.kafka.plain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.sme.kafka.plain.model.Config;

/**
 * Unit tests of {@link ConfigLoader}
 */
public class ConfigLoaderTest
{
    @Test
    void testLoadConfig() throws Exception
    {
        Config config = new ConfigLoader().load();
        assertEquals(ConfigTD.CONFIG, config);
    }

    /**
     * Config Test Data.
     */
    private static final class ConfigTD
    {
        static Config CONFIG = new Config();

        static
        {
            CONFIG.setHost("192.168.0.109:9092");
            CONFIG.setTopic("HelloKafkaTopic");
        }
    }
}
