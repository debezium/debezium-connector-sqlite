/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.CommonConnectorConfig;

/**
 * Integration test for {@link SQLiteSourceConnector#validateConnection} against real database files.
 */
class SQLiteSourceConnectorIT {

    @TempDir
    Path tempDir;

    @Test
    void validateReportsAnErrorWhenTheFileDoesNotExist() {
        Path missing = tempDir.resolve("does-not-exist.db");

        Config result = new SQLiteSourceConnector().validate(configFor(missing));

        assertThat(databaseFileErrors(result)).isNotEmpty();
    }

    @Test
    void validateWarnsButDoesNotFailWhenTheFileIsNotInWalMode() throws SQLException {
        Path notWal = tempDir.resolve("not-wal.db");
        createNonWalDatabase(notWal);

        Config result = new SQLiteSourceConnector().validate(configFor(notWal));

        assertThat(databaseFileErrors(result)).isEmpty();
    }

    private static Map<String, String> configFor(Path databaseFile) {
        return Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), databaseFile.toString(),
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test_prefix");
    }

    private static java.util.List<String> databaseFileErrors(Config result) {
        return result.configValues().stream()
                .filter(value -> value.name().equals(SQLiteConnectorConfig.DATABASE_FILE.name()))
                .findFirst()
                .map(ConfigValue::errorMessages)
                .orElseThrow();
    }

    private static void createNonWalDatabase(Path databaseFile) throws SQLException {
        // A freshly created SQLite file uses the default (rollback-journal) mode, not WAL.
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE probe (id INTEGER)");
        }
    }
}
