package com.sme.kafka.plain.admin;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.kafka.plain.model.Config;

/**
 * Works with Topic by admin API.
 */
public class AdminTopic
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminTopic.class);
    private Admin adminClient;

    public AdminTopic(Config config)
    {
        init(config);
    }

    private void init(Config config)
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost());
        adminClient = Admin.create(props);
    }

    /**
     * Fetch all topics.
     * 
     * @return Returns the set of topics.
     */
    public Set<String> list()
    {
        try
        {
            return adminClient.listTopics().names().get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException("Cannot fetch a list of topics");
        }
    }

    /**
     * Create Topics.
     * 
     * @param topics The give list of topics;
     * @return Returns true if topics created properly otherwise returns false.
     */
    public boolean createTopics(List<String> topics)
    {
        Set<String> createdTopics = list();
        List<String> filteredTopics = topics.stream().distinct().filter(topic -> !createdTopics.contains(topic)).collect(Collectors.toList());

        if (!filteredTopics.isEmpty())
        {
            LOGGER.debug("Create {} topics", filteredTopics);

            Collection<NewTopic> newTopics = topics.stream().map(n -> new NewTopic(n, 1, (short) 1)).collect(Collectors.toList());
            CreateTopicsResult result = adminClient.createTopics(newTopics);
            return !result.all().isCompletedExceptionally();
        }

        return true;
    }

    /**
     * Remove a list of topics.
     * 
     * @param topics The list of topics;
     * @return Returns true if the topics removed otherwise returns false.
     */
    public boolean removeTopics(List<String> topics)
    {
        Set<String> createdTopics = list();
        List<String> filteredTopics = topics.stream().distinct().filter(topic -> createdTopics.contains(topic)).collect(Collectors.toList());

        if (!filteredTopics.isEmpty())
        {
            DeleteTopicsResult result = adminClient.deleteTopics(filteredTopics);
            return !result.all().isCompletedExceptionally();
        }

        return true;
    }

    /**
     * Remove all topics.
     * 
     * @return Returns true if the topics removed otherwise returns false.
     */
    public boolean removeAllTopics()
    {
        DeleteTopicsResult result = adminClient.deleteTopics(list());
        return !result.all().isCompletedExceptionally();
    }
}
