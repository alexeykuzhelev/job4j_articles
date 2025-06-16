package ru.job4j.articles.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.job4j.articles.model.Article;
import ru.job4j.articles.model.Word;
import ru.job4j.articles.service.generator.ArticleGenerator;
import ru.job4j.articles.store.Store;

import java.util.stream.IntStream;

public class SimpleArticleService implements ArticleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleArticleService.class.getSimpleName());

    private final ArticleGenerator articleGenerator;

    public SimpleArticleService(ArticleGenerator articleGenerator) {
        this.articleGenerator = articleGenerator;
    }

    @Override
    public void generate(Store<Word> wordStore, int count, Store<Article> articleStore) {
        LOGGER.info("Геренация статей в количестве {}", count);
        var words = wordStore.findAll();
        IntStream.range(0, count)
                .mapToObj(i -> {
                    /* Логируем каждую 1000-ю статью */
                    if (i > 0 && i % 1000 == 0) {
                        LOGGER.info("Сгенерировано статей: {}", i);
                    }
                    return articleGenerator.generate(words);
                })
                .forEach(articleStore::save);
    }
}
