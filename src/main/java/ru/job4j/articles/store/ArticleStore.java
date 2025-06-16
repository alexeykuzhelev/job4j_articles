package ru.job4j.articles.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.job4j.articles.model.Article;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ArticleStore implements Store<Article>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArticleStore.class.getSimpleName());
    private static final int BATCH_SIZE = 1000;

    private final List<Article> buffer = new ArrayList<>(BATCH_SIZE);
    private final Properties properties;

    private Connection connection;

    public ArticleStore(Properties properties) {
        this.properties = properties;
        initConnection();
        initScheme();
    }

    private void initConnection() {
        LOGGER.info("Создание подключения к БД статей");
        try {
            connection = DriverManager.getConnection(
                    properties.getProperty("url"),
                    properties.getProperty("username"),
                    properties.getProperty("password")
            );
        } catch (SQLException throwables) {
            LOGGER.error("Не удалось выполнить операцию: { }", throwables.getCause());
            throw new IllegalStateException();
        }
    }

    private void initScheme() {
        LOGGER.info("Инициализация таблицы статей");
        try (var statement = connection.createStatement()) {
            var sql = Files.readString(Path.of("db/scripts", "articles.sql"));
            statement.execute(sql);
        } catch (Exception e) {
            LOGGER.error("Не удалось выполнить операцию: { }", e.getCause());
            throw new IllegalStateException();
        }
    }

    @Override
    public Article save(Article model) {
        buffer.add(model);
        if (buffer.size() >= BATCH_SIZE) {
            flush();
            System.gc();
        }
        return model;
    }

    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }

        var sql = "insert into articles(text) values(?)";
        try (var statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            /* Пакетная вставка записей в запрос */
            for (Article article : buffer) {
                statement.setString(1, article.getText());
                statement.addBatch();
            }

            statement.executeBatch();

            /* Обработка сгенерированных ключей */
            try (var key = statement.getGeneratedKeys()) {
                for (int i = 0; i < buffer.size(); i++) {
                    if (!key.next()) {
                        throw new SQLException("Не удалось получить сгенерированный ID");
                    }
                    int id = key.getInt(1);
                    buffer.get(i).setId(id);
                }
            }
            buffer.clear();
        } catch (Exception e) {
            LOGGER.error("Ошибка пакетной вставки записей в БД", e.getCause());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<Article> findAll() {
        LOGGER.info("Загрузка всех статей");
        var sql = "select * from articles";
        var articles = new ArrayList<Article>();
        try (var statement = connection.prepareStatement(sql)) {
            var selection = statement.executeQuery();
            while (selection.next()) {
                articles.add(new Article(
                        selection.getInt("id"),
                        selection.getString("text")
                ));
            }
        } catch (Exception e) {
            LOGGER.error("Не удалось выполнить операцию: { }", e.getCause());
            throw new IllegalStateException();
        }
        return articles;
    }

    @Override
    public void close() throws Exception {
        /* Сохраняем остатки записей и закрываем подключение к БД */
        flush();
        if (connection != null) {
            connection.close();
        }
    }
}
