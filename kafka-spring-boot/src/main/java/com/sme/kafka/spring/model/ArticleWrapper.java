package com.sme.kafka.spring.model;

/**
 * Represents ArticleWrapper domain.
 */
public class ArticleWrapper
{
    private final Article article;

    public ArticleWrapper(Article article)
    {
        this.article = article;
    }

    public Article getArticle()
    {
        return article;
    }
}
