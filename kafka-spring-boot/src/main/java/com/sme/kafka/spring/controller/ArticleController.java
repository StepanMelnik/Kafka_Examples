package com.sme.kafka.spring.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.sme.kafka.spring.exception.ResourceNotFoundException;
import com.sme.kafka.spring.persistence.ArticleRepository;
import com.sme.kafka.spring.service.ArticleProducerService;

/**
 * Article REST controller.
 */
@RestController
@RequestMapping(value = "v1/articles")
public class ArticleController
{
    @Value("${kafka.topics[0].name}")
    private String topic1;

    @Value("${kafka.topics[1].name}")
    private String topic2;

    @Value("${kafka.topics[2].name}")
    private String topic3;

    private final ArticleProducerService articleProducerService;
    private final ArticleRepository articleReposiory;

    ArticleController(ArticleProducerService articleProducerService, ArticleRepository articleReposiory)
    {
        this.articleProducerService = articleProducerService;
        this.articleReposiory = articleReposiory;
    }

    /**
     * Send a message with given article to Kafka container.
     * 
     * @param articleId The articleId parameter to fetch article.
     */
    @RequestMapping(value = "/{articleId}/send-message", method = RequestMethod.POST)
    public void sendMessage(@PathVariable("articleId") int articleId)
    {
        articleProducerService.sendMessage(topic1, articleReposiory.findById(articleId).orElseThrow(() -> new ResourceNotFoundException("Article not found with given id")));
    }

    //CSOFF
    /*
    @RequestMapping(value = "/{articleId}/send-message-with-reply", method = RequestMethod.POST)
    public ArticleWrapper sendMessageWithReply(@PathVariable("articleId") int articleId)
    {
        //return articleProducerService.sendMessageWithReply(topic1, articleReposiory.findById(articleId).orElseThrow(() -> new ResourceNotFoundException("Article not found with given id")));
        throw new UnsupportedOperationException("The operation has not supported yet");
    }
    */
    //CSON
}
