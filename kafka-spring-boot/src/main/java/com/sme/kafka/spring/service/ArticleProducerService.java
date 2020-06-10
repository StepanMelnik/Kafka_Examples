package com.sme.kafka.spring.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

import com.sme.kafka.spring.model.Article;

/**
 * Article producer service.
 */
@Service
public class ArticleProducerService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArticleProducerService.class);

    private final KafkaTemplate<String, Article> kafkaTemplate;

    public ArticleProducerService(KafkaTemplate<String, Article> kafkaTemplate)
    {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends a message.
     * 
     * @param topic The name of topic;
     * @param article The given article to send.
     */
    @Transactional
    public void sendMessage(String topic, Article article)
    {
        ListenableFuture<SendResult<String, Article>> send = kafkaTemplate.send(topic, article);
        send.addCallback(
                result ->
                {
                    LOGGER.info("Sent {} article to {} topic in {} partition", result.getProducerRecord().value(), result.getRecordMetadata().topic(), result.getRecordMetadata().partition());
                },
                ex ->
                {
                    LOGGER.error("Error occurred while sendig message： {}", ex.getMessage());
                });
    }

    //CSOFF
    /*
    @Transactional
    public ArticleWrapper sendMessageWithReply(String topic, Article article)
    {
        ProducerRecord<String, Article> producerRecord = new ProducerRecord<>(topic, article);
        producerRecord.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, topic.getBytes()));
        RequestReplyFuture<String, Article, ArticleWrapper> sendAndReceive = replyingKafkaTemplate.sendAndReceive(producerRecord);
        sendAndReceive.addCallback(
                result ->
                {
                    LOGGER.info("Sent {} value", result.value());
                },
                ex ->
                {
                    LOGGER.error("Error occurred while sendig message： {}", ex.getMessage());
                });
        // confirm if producer produced successfully
        try
        {
            SendResult<String, Article> sendResult = sendAndReceive.getSendFuture().get();
            sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value()));
        }
        catch (InterruptedException | ExecutionException e1)
        {
            System.out.println("error");
        }
        try
        {
            return sendAndReceive.get().value();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException("Article cannot be sent", e);
        }
    }
    */
    //CSON
}
