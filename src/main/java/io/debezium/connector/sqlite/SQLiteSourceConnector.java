/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;

/**
 * The top-level Kafka Connect source connector for SQLite.
 *
 * <p>Registers the connector with Kafka Connect and always runs exactly one
 * {@link SQLiteConnectorTask}.
 */
public class SQLiteSourceConnector extends RelationalBaseSourceConnector {

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
        // TODO: open a test JDBC connection to the SQLite file and report any errors in Phase 1.
    }
}
