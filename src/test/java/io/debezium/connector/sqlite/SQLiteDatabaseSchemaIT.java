/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Verifies that {@link SQLiteDatabaseSchema#refresh} reads a live multi-table database, registers a
 * schema for every monitored table, leaves the SQLite internals and the connector's own tables out,
 * and honors a {@code table.exclude.list} entry. Needs a real database, so it is an integration test.
 */
public class SQLiteDatabaseSchemaIT {

    private SQLiteDatabaseSchema buildSchema(SqliteTestHelper db, Map<String, String> extraConfig) {
        Map<String, String> props = new HashMap<>();
        props.put(SQLiteConnectorConfig.DATABASE_FILE.name(), db.databaseFile().toString());
        props.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "test");
        props.putAll(extraConfig);
        Configuration config = Configuration.from(props);
        SQLiteConnectorConfig connectorConfig = new SQLiteConnectorConfig(config);
        CdcSourceTaskContext<SQLiteConnectorConfig> taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        return new SQLiteDatabaseSchema(taskContext, topicNamingStrategy);
    }

    private SQLiteDatabaseSchema refreshOver(SqliteTestHelper db, Map<String, String> extraConfig) throws Exception {
        SQLiteDatabaseSchema schema = buildSchema(db, extraConfig);
        try (SQLiteConnection connection = new SQLiteConnection(db.databaseFile().toString())) {
            schema.refresh(connection);
        }
        return schema;
    }

    private boolean hasTable(SQLiteDatabaseSchema schema, String name) {
        return schema.tableIds().stream().anyMatch(id -> name.equals(id.table()));
    }

    private TableId idOf(SQLiteDatabaseSchema schema, String name) {
        return schema.tableIds().stream().filter(id -> name.equals(id.table())).findFirst().orElseThrow();
    }

    @Test
    void registersMonitoredTablesAndExcludesInternalTables() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
            db.connection().execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");

            SQLiteDatabaseSchema schema = refreshOver(db, Map.of());

            assertThat(hasTable(schema, "orders")).isTrue();
            assertThat(hasTable(schema, "customers")).isTrue();
            assertThat(hasTable(schema, CdcLog.TABLE_NAME)).isFalse();
            assertThat(schema.tableIds()).noneMatch(id -> id.table().startsWith("sqlite_"));

            assertThat(schema.schemaFor(idOf(schema, "orders")).valueSchema()).isNotNull();
            assertThat(schema.schemaFor(idOf(schema, "orders")).keySchema()).isNotNull();
        }
    }

    @Test
    void honorsTableExcludeList() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
            db.connection().execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");

            SQLiteDatabaseSchema schema = refreshOver(db,
                    Map.of(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST.name(), "customers"));

            assertThat(hasTable(schema, "orders")).isTrue();
            assertThat(hasTable(schema, "customers")).isFalse();
        }
    }
}
