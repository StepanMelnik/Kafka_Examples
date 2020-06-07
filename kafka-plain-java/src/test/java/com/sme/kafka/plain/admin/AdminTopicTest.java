package com.sme.kafka.plain.admin;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit tests of {@link AdminTopic}.
 */
public class AdminTopicTest
{
    private final AdminTopic adminTopic = new AdminTopic();

    @Test
    void testList() throws Exception
    {
        List<String> topics = asList("Topic1", "Topic2");
        boolean result = adminTopic.createTopics(topics);
        assertTrue(result, "Expects all created topics");

        assertTrue(adminTopic.list().containsAll(topics), "Expects that list contains created topics");
    }
}
