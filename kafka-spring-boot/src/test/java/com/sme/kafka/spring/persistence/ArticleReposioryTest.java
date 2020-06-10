package com.sme.kafka.spring.persistence;

import java.math.BigDecimal;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sme.kafka.spring.model.Article;
import com.sme.kafka.spring.util.PojoGenericBuilder;

/**
 * Unit tests of {@link ArticleRepository}.
 */
public class ArticleReposioryTest extends Assertions
{
    private ArticleRepository articleRepository;

    @BeforeEach
    public void setUp()
    {
        articleRepository = new ArticleRepository();
    }

    @Test
    void testFindById() throws Exception
    {
        assertEquals(ArticleTD.ARTICLE1, articleRepository.findById(100).get());
        assertThrows(NoSuchElementException.class, () -> articleRepository.findById(10000).get());
    }

    /**
     * Article test data.
     */
    private static class ArticleTD
    {
        private static Article ARTICLE1 = new PojoGenericBuilder<>(Article::new)
                .with(Article::setId, 100)
                .with(Article::setActive, true)
                .with(Article::setName, "Name100")
                .with(Article::setDescription, "Article Name 100")
                .with(Article::setPrice, BigDecimal.valueOf(10.01d))
                .build();
    }
}
