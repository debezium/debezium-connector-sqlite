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

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }
}
