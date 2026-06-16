/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.io.File;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;

/**
 * The top-level Kafka Connect source connector for SQLite.
 *
 * <p>Registers the connector with Kafka Connect and always runs exactly one
 * {@link SQLiteConnectorTask}.
 */
public class SQLiteSourceConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteSourceConnector.class);

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SQLiteConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single task is supported for the SQLite connector "
                    + "because SQLite allows a single writer; requested " + maxTasks + ".");
        }
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SQLiteConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(SQLiteConnectorConfig.ALL_FIELDS);
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        ConfigValue fileValue = configValues.get(SQLiteConnectorConfig.DATABASE_FILE.name());
        String path = config.getString(SQLiteConnectorConfig.DATABASE_FILE);

        File databaseFile = new File(path);
        if (!databaseFile.exists()) {
            fileValue.addErrorMessage("SQLite database file does not exist: " + path);
            return;
        }
        if (!databaseFile.canRead()) {
            fileValue.addErrorMessage("SQLite database file is not readable: " + path);
            return;
        }

        try (SQLiteConnection connection = new SQLiteConnection(path)) {
            connection.connect();
            String mode = connection.journalMode();
            if (!SQLiteConnection.JOURNAL_MODE_WAL.equalsIgnoreCase(mode)) {
                LOGGER.warn("SQLite database '{}' is in '{}' journal mode, not WAL. The connector will "
                        + "switch it to WAL when the task starts.", path, mode);
            }
        }
        catch (SQLException e) {
            fileValue.addErrorMessage("Unable to connect to SQLite database file '" + path + "': " + e.getMessage());
        }
    }
}
