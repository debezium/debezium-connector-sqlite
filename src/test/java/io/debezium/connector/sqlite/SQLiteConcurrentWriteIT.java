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
 * Integration test that the connector loses no change and preserves {@code change_id} order when many
 * writers insert into a monitored table at the same time. Each writer uses its own connection to the
 * same WAL database, so the inserts genuinely contend; the streamed records must still be the complete,
 * gap-free, ascending {@code change_id} sequence, and every written row must arrive exactly once.
 */
public class SQLiteConcurrentWriteIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_concurrent";
    private static final int WRITERS = 4;
    private static final int ROWS_PER_WRITER = 25;
    private static final int TOTAL = WRITERS * ROWS_PER_WRITER;

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
    public void shouldStreamEveryChangeInOrderUnderConcurrentWrites() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, writer INTEGER)");

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
            for (Future<?> future : done) {
                future.get(30, TimeUnit.SECONDS);
            }
        }
        finally {
            writers.shutdownNow();
        }

        List<SourceRecord> records = consumeRecordsByTopic(TOTAL, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(records).hasSize(TOTAL);

        // Delivery is in change_id order and covers the whole sequence with no gap, so nothing was lost
        // or reordered even though the writes contended.
        List<Long> changeIds = records.stream()
                .map(record -> source(record).getInt64("change_id"))
                .collect(Collectors.toList());
        assertThat(changeIds).isEqualTo(LongStream.rangeClosed(1, TOTAL).boxed().collect(Collectors.toList()));

        // Every row every writer wrote arrived exactly once.
        List<Long> ids = records.stream()
                .map(record -> after(record).getInt64("id"))
                .sorted()
                .collect(Collectors.toList());
        assertThat(ids).isEqualTo(LongStream.rangeClosed(1, TOTAL).boxed().collect(Collectors.toList()));
    }

    private void writeRows(int writer) {
        try (JdbcConnection connection = openWriter()) {
            connection.connect();
            // Wait rather than fail when another writer holds the write lock, so contention serializes
            // instead of surfacing SQLITE_BUSY.
            connection.execute("PRAGMA busy_timeout=10000");
            for (int i = 0; i < ROWS_PER_WRITER; i++) {
                int id = writer * ROWS_PER_WRITER + i + 1;
                connection.execute("INSERT INTO t (id, writer) VALUES (" + id + ", " + writer + ")");
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
