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
 * Integration test for live streaming. With the snapshot skipped so the connector starts at the log end,
 * an insert, update, and delete captured by the triggers must each stream out as the matching {@code c},
 * {@code u}, or {@code d} change event, with the right before and after values, the primary key, a blob
 * round-tripped through the tagged encoding, and a source block carrying the commit time, the change id,
 * and the table name.
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

        // The empty log fixes the resume point at 0. Wait for streaming to start there before writing, so
        // the writes land past the resume point and stream out rather than being skipped as backlog.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        // The task installs the triggers at startup, so these writes are captured into the CDC log.
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
