/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
 * Integration test for live streaming. With the snapshot skipped so streaming starts at the log end, an
 * insert, update, and delete must each stream out as the matching {@code c}, {@code u}, {@code d} event
 * with the right before and after values, the key, a round-tripped blob, and a populated source block.
 */
public class SQLiteStreamingIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_stream";

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
    public void shouldStreamInsertUpdateAndDelete() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, data BLOB)");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        // The empty log fixes the resume point at 0. Wait for streaming to start before writing, so the
        // writes are not skipped as backlog.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        // The task installed the triggers at startup, so these writes are captured.
        database.connection().execute("INSERT INTO products (id, name, data) VALUES (1, 'widget', x'0102')");
        database.connection().execute("UPDATE products SET name = 'gadget' WHERE id = 1");
        database.connection().execute("DELETE FROM products WHERE id = 1");

        List<SourceRecord> records = consumeRecordsByTopic(3, false).recordsForTopic(TOPIC_PREFIX + ".products");
        assertThat(records).hasSize(3);

        SourceRecord insert = records.get(0);
        SourceRecord update = records.get(1);
        SourceRecord delete = records.get(2);

        assertThat(operation(insert)).isEqualTo(Envelope.Operation.CREATE.code());
        assertThat(operation(update)).isEqualTo(Envelope.Operation.UPDATE.code());
        assertThat(operation(delete)).isEqualTo(Envelope.Operation.DELETE.code());

        // Every record is keyed by the integer primary key.
        assertThat(((Struct) insert.key()).getInt64("id")).isEqualTo(1L);

        // Insert: after holds the new row, before is absent, and the blob round-trips to its bytes.
        assertThat(before(insert)).isNull();
        assertThat(after(insert).getString("name")).isEqualTo("widget");
        assertThat(after(insert).getBytes("data")).isEqualTo(new byte[]{ 1, 2 });

        // Update: before is the old row, after the new.
        assertThat(before(update).getString("name")).isEqualTo("widget");
        assertThat(after(update).getString("name")).isEqualTo("gadget");

        // Delete: before is the last row, after is absent.
        assertThat(before(delete).getString("name")).isEqualTo("gadget");
        assertThat(after(delete)).isNull();

        // Source block: change_id advances one per row, the table name is set, and ts_ms is the commit time.
        assertThat(records.stream().map(record -> source(record).getInt64("change_id")).collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L);
        assertThat(source(insert).getString("table")).isEqualTo("products");
        assertThat(source(insert).getInt64("ts_ms")).isPositive();
    }

    @Test
    public void shouldDrainABacklogInChangeIdOrderAcrossBatches() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER)");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                // A small batch forces the five changes to drain over several polls, so the test proves
                // the order holds across batch boundaries.
                .with(SQLiteConnectorConfig.CDC_LOG_BATCH_SIZE, 2)
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        for (int i = 1; i <= 5; i++) {
            database.connection().execute("INSERT INTO t (id, seq) VALUES (" + i + ", " + i + ")");
        }

        List<SourceRecord> streamed = consumeRecordsByTopic(5, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(streamed).hasSize(5);
        assertThat(streamed.stream().map(record -> source(record).getInt64("change_id")).collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(streamed.stream().map(record -> after(record).getInt64("id")).collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    private static String operation(SourceRecord record) {
        return ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
    }

    private static Struct before(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static Struct source(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
    }
}
