/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * The showable goal of the phase: derive the Kafka Connect key and value schemas for a sample table
 * straight from a live database, with every SQLite affinity mapped to its Connect type, and a
 * {@code column.exclude.list} entry dropping a column from the value schema. Needs a real database,
 * so it is an integration test.
 */
public class SQLiteSchemaDerivationIT {

    private static final String PRODUCTS = "CREATE TABLE products ("
            + "  id INTEGER PRIMARY KEY,"
            + "  name TEXT,"
            + "  price REAL,"
            + "  qty BIGINT,"
            + "  data BLOB,"
            + "  rating DECIMAL(5,2)"
            + ")";

    private TableSchema deriveProducts(SqliteTestHelper db, Map<String, String> extraConfig) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(SQLiteConnectorConfig.DATABASE_FILE.name(), db.databaseFile().toString());
        props.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "test");
        props.putAll(extraConfig);
        Configuration config = Configuration.from(props);
        SQLiteConnectorConfig connectorConfig = new SQLiteConnectorConfig(config);
        CdcSourceTaskContext<SQLiteConnectorConfig> taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        SQLiteDatabaseSchema schema = new SQLiteDatabaseSchema(taskContext, topicNamingStrategy);
        try (SQLiteConnection connection = new SQLiteConnection(db.databaseFile().toString())) {
            schema.refresh(connection);
        }
        TableId id = schema.tableIds().stream().filter(t -> "products".equals(t.table())).findFirst().orElseThrow();
        return schema.schemaFor(id);
    }

    @Test
    void derivesValueAndKeySchemasFromAffinity() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute(PRODUCTS);

            TableSchema schema = deriveProducts(db, Map.of());

            Schema value = schema.valueSchema();
            assertThat(value.field("id").schema().type()).isEqualTo(Schema.Type.INT64);
            assertThat(value.field("name").schema().type()).isEqualTo(Schema.Type.STRING);
            assertThat(value.field("price").schema().type()).isEqualTo(Schema.Type.FLOAT64);
            assertThat(value.field("qty").schema().type()).isEqualTo(Schema.Type.INT64);
            assertThat(value.field("data").schema().type()).isEqualTo(Schema.Type.BYTES);
            assertThat(value.field("rating").schema().type()).isEqualTo(Schema.Type.FLOAT64);

            Schema key = schema.keySchema();
            assertThat(key.fields()).hasSize(1);
            assertThat(key.field("id").schema().type()).isEqualTo(Schema.Type.INT64);
        }
    }

    @Test
    void columnExcludeListDropsColumnFromValueSchema() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute(PRODUCTS);

            TableSchema schema = deriveProducts(db,
                    Map.of(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST.name(), "products.name"));

            assertThat(schema.valueSchema().field("name")).isNull();
            assertThat(schema.valueSchema().field("price")).isNotNull();
        }
    }
}
