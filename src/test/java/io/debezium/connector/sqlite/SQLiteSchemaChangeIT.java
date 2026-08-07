/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
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
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration tests for keeping the capture triggers in step with the schema. A column added under an
 * installed trigger is not captured until the trigger is rebuilt from the new column list, and these
 * tests prove the connector rebuilds it, both at startup and while streaming.
 */
public class SQLiteSchemaChangeIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_schema";

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
    public void shouldCaptureAColumnAddedWhileStopped() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        // A previous run installed triggers for orders(id, name). While the connector was down a column
        // was added, so the installed trigger still captures only id and name and would silently drop the
        // new column until the triggers are rebuilt from the current column list.
        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");
        database.connection().execute("ALTER TABLE orders ADD COLUMN note TEXT");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        // Streaming starts only after the task's startup reconcile has run, so by now the triggers are
        // rebuilt for orders(id, name, note).
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        database.connection().execute("INSERT INTO orders (id, name, note) VALUES (1, 'a', 'hello')");

        // Without the startup reconcile the stale trigger would omit note and it would decode to null.
        List<SourceRecord> records = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".orders");
        assertThat(records).hasSize(1);
        assertThat(after(records.get(0)).getString("note")).isEqualTo("hello");
    }

    @Test
    public void shouldCaptureAColumnAddedWhileStreaming() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);
        LogInterceptor reconcileLog = new LogInterceptor(TriggerReconciler.class);

        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        // Install the triggers up front so the startup reconcile is a no-op. That keeps the only "Rebuilt"
        // log the mid-stream one this test waits on.
        database.installTriggers("orders");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        database.connection().execute("INSERT INTO orders (id, name) VALUES (1, 'a')");

        // Add a column mid-stream and wait for the poll loop to notice the schema_version bump and rebuild
        // the trigger, so the next write is captured with the new column rather than racing the reconcile.
        database.connection().execute("ALTER TABLE orders ADD COLUMN note TEXT");
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> reconcileLog.containsMessage("Rebuilt the capture triggers for table 'orders'"));

        database.connection().execute("INSERT INTO orders (id, name, note) VALUES (2, 'b', 'hello')");

        List<SourceRecord> records = consumeRecordsByTopic(2, false).recordsForTopic(TOPIC_PREFIX + ".orders");
        assertThat(records).hasSize(2);
        assertThat(after(records.get(1)).getString("note")).isEqualTo("hello");
    }

    @Test
    public void shouldNotRebuildTriggersForAnUnrelatedSchemaChange() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);
        LogInterceptor reconcileLog = new LogInterceptor(TriggerReconciler.class);

        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        database.connection().execute("INSERT INTO orders (id, name) VALUES (1, 'a')");
        // A CREATE INDEX bumps schema_version but changes no table's columns. The poll loop reconciles to
        // a no-op, and streaming carries on.
        database.connection().execute("CREATE INDEX idx_orders_name ON orders (name)");
        database.connection().execute("INSERT INTO orders (id, name) VALUES (2, 'b')");

        List<SourceRecord> records = consumeRecordsByTopic(2, false).recordsForTopic(TOPIC_PREFIX + ".orders");
        assertThat(records).hasSize(2);
        assertThat(after(records.get(1)).getString("name")).isEqualTo("b");
        // Consuming the row written after the index guarantees the poll loop saw the bump, so no rebuild
        // means the bump was correctly treated as a no-op.
        assertThat(reconcileLog.containsMessage("Rebuilt the capture triggers for table 'orders'")).isFalse();
    }

    @Test
    public void shouldCaptureANewTableCreatedWhileStreaming() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        // A table created mid-stream must get triggers from the reconcile and then stream.
        database.connection().execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT)");
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> triggerCountFor("audit") == 3);

        database.connection().execute("INSERT INTO audit (id, note) VALUES (1, 'x')");

        List<SourceRecord> records = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".audit");
        assertThat(records).hasSize(1);
        assertThat(after(records.get(0)).getString("note")).isEqualTo("x");
    }

    @Test
    public void shouldKeepStreamingAcrossARenameWithoutDoubleCapture() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        // Stream the pre-rename change under the old name first, so its topic is not raced by the rename.
        database.connection().execute("INSERT INTO orders (id, name) VALUES (1, 'a')");
        assertThat(consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".orders")).hasSize(1);

        database.connection().execute("ALTER TABLE orders RENAME TO sales");
        // The reconcile installs triggers for the new name and drops the old name's triggers. Wait for
        // both, so the next write can only fire the new trigger.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> triggerCountFor("sales") == 3 && triggerCountFor("orders") == 0);

        long before = maxChangeId();
        database.connection().execute("INSERT INTO sales (id, name) VALUES (2, 'b')");
        // No double capture: the single insert produced exactly one CDC row, under the new name.
        assertThat(tablesLoggedAfter(before)).containsExactly("sales");

        List<SourceRecord> sales = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".sales");
        assertThat(sales).hasSize(1);
        assertThat(after(sales.get(0)).getString("name")).isEqualTo("b");
    }

    @Test
    public void shouldSkipARowForAnUnmonitoredTableAndKeepStreaming() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
        database.installTriggers("orders");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        // A leftover CDC row for a table the connector does not monitor, as a rename or drop would leave.
        database.connection().execute("INSERT INTO " + CdcLog.TABLE_NAME
                + " (table_name, operation, new_row_data, committed_at) VALUES ('ghost', 'c', '{\"id\":1}', 0)");
        database.connection().execute("INSERT INTO orders (id, name) VALUES (1, 'a')");

        // The ghost row is skipped and streaming carries on to the real change.
        List<SourceRecord> records = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".orders");
        assertThat(records).hasSize(1);
        assertThat(after(records.get(0)).getString("name")).isEqualTo("a");
        assertThat(streamingLog.containsMessage("Skipping change")).isTrue();
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private long maxChangeId() throws SQLException {
        return database.connection().queryAndMap(
                "SELECT COALESCE(MAX(" + CdcLog.CHANGE_ID + "), 0) FROM " + CdcLog.TABLE_NAME,
                rs -> rs.next() ? rs.getLong(1) : 0L);
    }

    private List<String> tablesLoggedAfter(long changeId) throws SQLException {
        return database.connection().queryAndMap(
                "SELECT " + CdcLog.TABLE_NAME_COLUMN + " FROM " + CdcLog.TABLE_NAME
                        + " WHERE " + CdcLog.CHANGE_ID + " > " + changeId + " ORDER BY " + CdcLog.CHANGE_ID,
                rs -> {
                    List<String> tables = new ArrayList<>();
                    while (rs.next()) {
                        tables.add(rs.getString(1));
                    }
                    return tables;
                });
    }

    private long triggerCountFor(String table) throws SQLException {
        String prefix = "_debezium_cdc_" + table + "_";
        return database.connection().queryAndMap("SELECT name FROM sqlite_master WHERE type='trigger'", rs -> {
            long count = 0;
            while (rs.next()) {
                if (rs.getString(1).startsWith(prefix)) {
                    count++;
                }
            }
            return count;
        });
    }
}
