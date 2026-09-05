/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration test that an {@code ALTER TABLE ADD COLUMN} run while writers are inserting concurrently
 * loses no row and no existing-column data: every concurrent insert into the original columns still
 * streams out across the trigger rebuild. Once the connector has reconciled, a write that sets the new
 * column is captured with it. A write in the brief window between the alter and the rebuild is captured
 * by the still-stale trigger, so the test writes the new-column row only after awaiting the reconcile,
 * matching how a mid-stream add-column is handled.
 */
public class SQLiteSchemaChangeUnderLoadIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_alter_load";
    private static final int WRITERS = 3;
    private static final int ROWS_PER_WRITER = 20;
    private static final int CONCURRENT_ROWS = WRITERS * ROWS_PER_WRITER;
    private static final long NEW_COLUMN_ROW_ID = 1000L;

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
    public void shouldNotLoseColumnsWhenAlteringUnderConcurrentWrites() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);
        LogInterceptor reconcileLog = new LogInterceptor(TriggerReconciler.class);

        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)");
        // Install the triggers up front so the startup reconcile is a no-op; the only "Rebuilt" log is
        // then the post-alter one, which the test waits on.
        database.installTriggers("t");
        // The alter contends with the writers for the write lock; wait for it rather than fail.
        database.connection().execute("PRAGMA busy_timeout=10000");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        ExecutorService writers = Executors.newFixedThreadPool(WRITERS);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (int w = 0; w < WRITERS; w++) {
                final int writer = w;
                done.add(writers.submit(() -> writeRows(writer)));
            }

            // Alter mid-flight, then wait for the connector to rebuild the trigger before writing the
            // row that sets the new column, so that row is captured with it rather than racing the rebuild.
            database.connection().execute("ALTER TABLE t ADD COLUMN b TEXT");
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> reconcileLog.containsMessage("Rebuilt the capture triggers for table 't'"));
            database.connection().execute(
                    "INSERT INTO t (id, a, b) VALUES (" + NEW_COLUMN_ROW_ID + ", 'x', 'new')");

            for (Future<?> future : done) {
                future.get(30, TimeUnit.SECONDS);
            }
        }
        finally {
            writers.shutdownNow();
        }

        // The mid-stream ALTER also dispatches one schema change event on the bare prefix topic, so the
        // total consumed is the data rows plus that one event; the data topic holds only the data rows.
        int dataRows = CONCURRENT_ROWS + 1;
        List<SourceRecord> records = consumeRecordsByTopic(dataRows + 1, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(records).hasSize(dataRows);

        // No row was lost across the alter: the change_id sequence is complete and gap-free.
        List<Long> changeIds = records.stream()
                .map(record -> source(record).getInt64("change_id"))
                .collect(Collectors.toList());
        assertThat(changeIds).isEqualTo(LongStream.rangeClosed(1, dataRows).boxed().collect(Collectors.toList()));

        // Every concurrent row kept its original-column value; none was dropped when the trigger rebuilt.
        List<Long> concurrentIds = records.stream()
                .map(record -> after(record).getInt64("id"))
                .filter(id -> id != NEW_COLUMN_ROW_ID)
                .sorted()
                .collect(Collectors.toList());
        assertThat(concurrentIds).isEqualTo(LongStream.rangeClosed(1, CONCURRENT_ROWS).boxed().collect(Collectors.toList()));
        assertThat(records.stream()
                .filter(record -> after(record).getInt64("id") != NEW_COLUMN_ROW_ID)
                .allMatch(record -> after(record).getString("a") != null)).isTrue();

        // The row written after the reconcile carries the new column.
        Struct newColumnRow = records.stream()
                .map(SQLiteSchemaChangeUnderLoadIT::after)
                .filter(after -> after.getInt64("id") == NEW_COLUMN_ROW_ID)
                .findFirst()
                .orElseThrow();
        assertThat(newColumnRow.getString("b")).isEqualTo("new");
    }

    private void writeRows(int writer) {
        try (JdbcConnection connection = openWriter()) {
            connection.connect();
            connection.execute("PRAGMA busy_timeout=10000");
            for (int i = 0; i < ROWS_PER_WRITER; i++) {
                int id = writer * ROWS_PER_WRITER + i + 1;
                connection.execute("INSERT INTO t (id, a) VALUES (" + id + ", 'w" + writer + "-" + i + "')");
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Writer " + writer + " failed", e);
        }
    }

    private JdbcConnection openWriter() {
        return new JdbcConnection(
                JdbcConfiguration.empty(),
                config -> DriverManager.getConnection("jdbc:sqlite:" + database.databaseFile()),
                "\"", "\"");
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static Struct source(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
    }
}
