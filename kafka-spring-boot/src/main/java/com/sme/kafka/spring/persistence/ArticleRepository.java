package com.sme.kafka.spring.persistence;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.sme.kafka.spring.model.Article;
import com.sme.kafka.spring.util.PojoGenericBuilder;

/**
 * Article repository in memory.
 */
@Service
public class ArticleRepository
{
    /**
     * Find {@link Article} by id.
     * 
     * @param id The id of article;
     * @return Returns an article by id.
     */
    public Optional<Article> findById(int id)
    {
        return ArticleTD.ARTICLES.stream().filter(a -> a.getId() == id).findFirst();
    }

    /**
     * Article Data in memory.
     */
    //CSOFF
    private static class ArticleTD
    {
        private static List<Article> ARTICLES = Arrays.asList(
                new PojoGenericBuilder<>(Article::new)
                        .with(Article::setId, 1)
                        .with(Article::setActive, true)
                        .with(Article::setName, "Name1_" + UUID.randomUUID())
                        .with(Article::setDescription, "Article Name 1")
                        .with(Article::setPrice, BigDecimal.valueOf(10.01d))
                        .build(),
                new PojoGenericBuilder<>(Article::new)
                        .with(Article::setId, 2)
                        .with(Article::setActive, true)
                        .with(Article::setName, "Name2_" + UUID.randomUUID())
                        .with(Article::setDescription, "Article Name 2")
                        .with(Article::setPrice, BigDecimal.valueOf(12.31d))
                        .build(),
                new PojoGenericBuilder<>(Article::new)
                        .with(Article::setId, 3)
                        .with(Article::setActive, true)
                        .with(Article::setName, "Name3_" + UUID.randomUUID())
                        .with(Article::setDescription, "Article Name 3")
                        .with(Article::setPrice, BigDecimal.valueOf(15.01d))
                        .build(),
                new PojoGenericBuilder<>(Article::new)
                        .with(Article::setId, 4)
                        .with(Article::setActive, true)
                        .with(Article::setName, "Name4_" + UUID.randomUUID())
                        .with(Article::setDescription, "Article Name 4")
                        .with(Article::setPrice, BigDecimal.valueOf(22.21d))
                        .build(),
                new PojoGenericBuilder<>(Article::new)
                        .with(Article::setId, 5)
                        .with(Article::setActive, true)
                        .with(Article::setName, "Name5_" + UUID.randomUUID())
                        .with(Article::setDescription, "Article Name 5")
                        .with(Article::setPrice, BigDecimal.valueOf(11.22d))
                        .build(),
                new PojoGenericBuilder<>(Article::new)
                        .with(Article::setId, 100)
                        .with(Article::setActive, true)
                        .with(Article::setName, "Name100")
                        .with(Article::setDescription, "Article Name 100")
                        .with(Article::setPrice, BigDecimal.valueOf(10.01d))
                        .build());
    }
    //CSON
}
