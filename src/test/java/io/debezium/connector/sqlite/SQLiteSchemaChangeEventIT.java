/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration tests for the {@code SchemaChangeEvent}s the connector emits while streaming, once the
 * trigger reconciler in {@link SQLiteSchemaChangeIT} picks up a mid-stream DDL.
 */
public class SQLiteSchemaChangeEventIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_schema_event";

    private SqliteTestHelper database;

    @BeforeEach
    public void prepareDatabase() throws Exception {
        database = SqliteTestHelper.create();
    }

    @AfterEach
    public void closeDatabase() throws Exception {
        stopConnector();
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void shouldEmitAnAlterEventWhenAColumnIsAddedWhileStreaming() throws Exception {
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        startStreaming();

        database.connection().execute("ALTER TABLE orders ADD COLUMN note TEXT");

        Struct value = awaitSchemaChangeEvent();
        assertThat(value.getString("ddl")).isNull();
        List<Struct> tableChanges = value.getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertThat(tableChanges.get(0).get("type")).isEqualTo("ALTER");
        assertThat(columnNames(tableChanges.get(0))).contains("note");
    }

    @Test
    public void shouldEmitAnAlterEventWhenAColumnIsRenamedWhileStreaming() throws Exception {
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        startStreaming();

        database.connection().execute("ALTER TABLE orders RENAME COLUMN name TO customer_name");

        Struct value = awaitSchemaChangeEvent();
        List<Struct> tableChanges = value.getArray("tableChanges");
        assertThat(tableChanges.get(0).get("type")).isEqualTo("ALTER");
        assertThat(columnNames(tableChanges.get(0))).containsExactlyInAnyOrder("id", "customer_name");
    }

    @Test
    public void shouldEmitACreateEventForANewTableCreatedWhileStreaming() throws Exception {
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        startStreaming();

        database.connection().execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)");

        Struct value = awaitSchemaChangeEvent();
        List<Struct> tableChanges = value.getArray("tableChanges");
        assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");
        assertThat(columnNames(tableChanges.get(0))).containsExactlyInAnyOrder("id", "note");
    }

    @Test
    public void shouldEmitADropEventForATableDroppedWhileStreaming() throws Exception {
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        startStreaming();

        database.connection().execute("DROP TABLE orders");

        Struct value = awaitSchemaChangeEvent();
        List<Struct> tableChanges = value.getArray("tableChanges");
        assertThat(tableChanges.get(0).get("type")).isEqualTo("DROP");
    }

    @Test
    public void shouldEmitADropThenACreateEventForATableRenamedWhileStreaming() throws Exception {
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        startStreaming();

        database.connection().execute("ALTER TABLE orders RENAME TO sales_orders");

        // The rename produces two separate events, since SQLite gives no link between the old and new
        // name: a DROP for the vanished old name and a CREATE for what looks like a fresh table.
        List<SourceRecord> events = consumeRecordsByTopic(2, false).recordsForTopic(TOPIC_PREFIX);
        assertThat(events).hasSize(2);
        List<String> types = events.stream()
                .map(record -> (String) ((Struct) record.value()).<Struct> getArray("tableChanges").get(0).get("type"))
                .toList();
        assertThat(types).containsExactlyInAnyOrder("DROP", "CREATE");
    }

    @Test
    public void shouldDispatchTheSchemaChangeEventBeforeTheDataEventInTheSamePollIteration() throws Exception {
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        startStreaming();

        // Both the DDL and the write that depends on it land before the poll loop's next iteration, so
        // the schema change event and the data event are produced by the same reconcile-then-batch pass.
        database.connection().execute("ALTER TABLE orders ADD COLUMN note TEXT");
        database.connection().execute("INSERT INTO orders (id, name, note) VALUES (1, 'a', 'hello')");

        SourceRecords records = consumeRecordsByTopic(2, false);
        SourceRecord schemaChange = records.recordsForTopic(TOPIC_PREFIX).get(0);
        SourceRecord dataChange = records.recordsForTopic(TOPIC_PREFIX + ".orders").get(0);
        assertThat(records.allRecordsInOrder()).containsExactly(schemaChange, dataChange);
    }

    private void startStreaming() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);
        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));
    }

    private Struct awaitSchemaChangeEvent() throws Exception {
        List<SourceRecord> events = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX);
        assertThat(events).hasSize(1);
        return (Struct) events.get(0).value();
    }

    private static List<String> columnNames(Struct tableChange) {
        List<Struct> columns = ((Struct) tableChange.get("table")).getArray("columns");
        return columns.stream().map(column -> column.getString("name")).toList();
    }
}
