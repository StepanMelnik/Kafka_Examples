package com.sme.kafka.spring.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.sme.kafka.spring.model.Article;

/**
 * Unit tests of {@link ArticleProducerService} and {@link ArticleConsumerService} pair.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest
@DirtiesContext
public class ArticleProducerConsumerServiceTest extends Assertions
{
    @Autowired
    private ArticleConsumerService articleConsumerService;

    @Autowired
    private ArticleProducerService articleProducerService;

    /**
     * @see application.yml
     */
    @Value("${kafka.topics[0].name}")
    private String topic1;

    @Test
    public void testSendMessage() throws InterruptedException
    {
        articleProducerService.sendMessage(topic1, new Article());

        Thread.sleep(10000); // sleep to get async message by consumer
        Article actualArticle = articleConsumerService.pollArticle();
        assertEquals(new Article(), actualArticle);
    }
}
