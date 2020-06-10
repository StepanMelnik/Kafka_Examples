package com.sme.kafka.spring.service;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sme.kafka.spring.model.Article;

/**
 * Article Consumer service.
 */
@Service
public class ArticleConsumerService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArticleProducerService.class);

    private final BlockingQueue<Article> queue = new ArrayBlockingQueue<>(2);

    @Value("${kafka.topics[0].name}")
    private String topic1;

    @Value("${kafka.topics[1].name}")
    private String topic2;

    @Value("${kafka.topics[2].name}")
    private String topic3;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Kafka Listener to consume events.
     * 
     * @param articleConsumerRecord The consumer record;
     * @param key The Key header in meta data;
     * @param partition The number of partition in meta data;
     * @param topic The name of topic in meta data;
     * @param ts The timestamp in meta data.
     */
    @KafkaListener(topics = {"${kafka.topics[0].name}"}, groupId = "GROUP1")
    @SendTo
    public void consumeGroup1Message(ConsumerRecord<String, String> articleConsumerRecord,
            @Header(name = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) Integer key,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts)
    {
        try
        {
            Article article = objectMapper.readValue(articleConsumerRecord.value(), Article.class);
            LOGGER.debug("Metadata: {} key, {} partition, {} topic, {} timestamp", key, partition, topic, ts);
            LOGGER.info("Transform a record in {} topic and {} partition -> {}", articleConsumerRecord.topic(), articleConsumerRecord.partition(), article);

            queue.add(article);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException("Cannot process json request", e);
        }
    }

    @KafkaListener(topics = {"${kafka.topics[1].name}"}, groupId = "GROUP2")
    public void consumeGroup2Message(Article article)
    {
        LOGGER.info("Consume artcile in {} topic", topic2, article);
    }

    /**
     * Returns the last added article in the queue.
     * 
     * @return
     */
    //@ForTestVisible
    Article pollArticle()
    {
        return queue.poll();
    }
}
