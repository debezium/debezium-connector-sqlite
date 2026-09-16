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

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration test proving the CDC log is compacted in bounded batches, keyed off the {@code change_id}
 * Kafka Connect has durably committed, never one merely dispatched.
 */
public class SQLiteCompactionIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_compaction";

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
    public void shouldDeleteRowsUpToTheCommittedChangeIdOnceTheThresholdIsCrossed() throws Exception {
        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        // Captured only by the snapshot, so the watermark is fixed at change_id 0 before any CDC row
        // exists.
        database.connection().execute("INSERT INTO t (id, name) VALUES (1, 'a')");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "initial")
                .with(SQLiteConnectorConfig.LOG_COMPACTION_THRESHOLD, 2)
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        assertThat(consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".t")).hasSize(1);

        database.connection().execute(
                "INSERT INTO t (id, name) VALUES (2, 'b')",
                "INSERT INTO t (id, name) VALUES (3, 'c')",
                "INSERT INTO t (id, name) VALUES (4, 'd')");

        List<SourceRecord> streamed = consumeRecordsByTopic(3, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(streamed).hasSize(3);

        // The test harness commits offsets essentially as soon as a record is handed to the consumer, so
        // change_id 1 through 3 are all committed by the time the poll loop next checks the threshold of
        // 2, and the delete removes every row. SQLiteConnectionIT proves the exact boundary
        // (deleteChangesUpTo never touches a row above the id it is given); this asserts the wiring
        // between commitOffset and the poll loop actually runs the delete end to end.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(remainingChangeIds()).isEmpty());
    }

    @Test
    public void shouldNotDeleteAnythingBeforeTheThresholdIsCrossed() throws Exception {
        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        database.connection().execute("INSERT INTO t (id, name) VALUES (1, 'a')");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "initial")
                .with(SQLiteConnectorConfig.LOG_COMPACTION_THRESHOLD, 100)
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        assertThat(consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".t")).hasSize(1);

        database.connection().execute("INSERT INTO t (id, name) VALUES (2, 'b')");
        assertThat(consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".t")).hasSize(1);

        // Give the poll loop a few idle iterations to run compactIfNeeded, then confirm it stayed a
        // no-op since only 1 row has committed against a threshold of 100.
        Thread.sleep(1000);
        assertThat(remainingChangeIds()).containsExactly(1L);
    }

    private List<Long> remainingChangeIds() throws SQLException {
        return database.connection().queryAndMap(
                "SELECT " + CdcLog.CHANGE_ID + " FROM " + CdcLog.TABLE_NAME + " ORDER BY " + CdcLog.CHANGE_ID,
                rs -> {
                    List<Long> ids = new ArrayList<>();
                    while (rs.next()) {
                        ids.add(rs.getLong(1));
                    }
                    return ids;
                });
    }
}
