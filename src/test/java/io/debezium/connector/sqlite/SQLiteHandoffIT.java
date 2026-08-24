/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration test for the snapshot-to-streaming handoff, the phase's correctness crux. The snapshot
 * fixes the largest {@code change_id} visible in its read view as the high-water mark, and streaming
 * resumes at the first change past it. A change committed after that mark must be delivered by streaming
 * exactly once and must never appear in the snapshot data, so the handoff has no gaps and no duplicates.
 */
public class SQLiteHandoffIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_handoff";

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
    public void shouldDeliverAPostWatermarkChangeOnceWithNoDuplicateOfASnapshotRow() throws Exception {
        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        database.connection().execute(
                "INSERT INTO t (id, name) VALUES (1, 'a')",
                "INSERT INTO t (id, name) VALUES (2, 'b')");
        // Pre-existing CDC log rows fix the watermark at 9, so streaming must resume past them.
        insertCdcLogRow(5);
        insertCdcLogRow(9);

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "initial")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        // Consuming the snapshot records first guarantees the snapshot has run and fixed the watermark, so
        // the change written next is unambiguously after it. 2 data rows plus 1 schema change record.
        List<SourceRecord> snapshotRows = consumeRecordsByTopic(3, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(snapshotRows).hasSize(2);
        assertThat(snapshotRows).allSatisfy(record -> assertThat(operation(record)).isEqualTo(Envelope.Operation.READ.code()));
        assertThat(snapshotRows).extracting(record -> ((Struct) record.key()).getInt64("id")).containsExactlyInAnyOrder(1L, 2L);

        // A change committed after the watermark. Its change_id is 10, past the mark of 9.
        database.connection().execute("INSERT INTO t (id, name) VALUES (3, 'c')");

        List<SourceRecord> streamed = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(streamed).hasSize(1);

        SourceRecord change = streamed.get(0);
        // Delivered once as a create, with the new row, resuming past the watermark. Id 3 was never
        // snapshotted, so there is no duplicate.
        assertThat(operation(change)).isEqualTo(Envelope.Operation.CREATE.code());
        assertThat(after(change).getInt64("id")).isEqualTo(3L);
        assertThat(after(change).getString("name")).isEqualTo("c");
        assertThat(source(change).getInt64("change_id")).isEqualTo(10L);
    }

    private void insertCdcLogRow(long changeId) throws SQLException {
        database.connection().execute(String.format(
                "INSERT INTO %s (%s, %s, %s, %s) VALUES (%d, 't', '%s', 0)",
                CdcLog.TABLE_NAME, CdcLog.CHANGE_ID, CdcLog.TABLE_NAME_COLUMN, CdcLog.OPERATION,
                CdcLog.COMMITTED_AT, changeId, CdcLog.OPERATION_CREATE));
    }

    private static String operation(SourceRecord record) {
        return ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static Struct source(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
    }
}
