/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Integration test proving that streaming resumes from the last committed {@code change_id} after a
 * restart. Per DDD-44 §3.5, the offset is the {@code change_id} of the last row Kafka Connect has
 * durably committed, and on restart the connector resumes with {@code change_id > <committed>}. A
 * stop-then-restart cycle must lose no row.
 */
public class SQLiteRestartIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_restart";

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
    public void shouldResumeStreamingFromTheCommittedChangeIdAfterARestart() throws Exception {
        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        // Written before the connector starts, so it is captured only by the snapshot, not by a
        // trigger. Consuming it below is a deterministic barrier: streaming cannot start until the
        // snapshot that produces it has finished.
        database.connection().execute("INSERT INTO t (id, name) VALUES (1, 'a')");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "initial")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        List<SourceRecord> snapshotRows = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(snapshotRows).hasSize(1);

        database.connection().execute(
                "INSERT INTO t (id, name) VALUES (2, 'b')",
                "INSERT INTO t (id, name) VALUES (3, 'c')");

        List<SourceRecord> beforeRestart = consumeRecordsByTopic(2, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(beforeRestart).hasSize(2);
        assertThat(beforeRestart).extracting(record -> ((Struct) record.key()).getInt64("id")).containsExactlyInAnyOrder(2L, 3L);

        // A clean stop flushes the offset commit for both rows already consumed above.
        stopConnector();

        // Written while the connector is stopped, so it must be picked up by the restart, not lost.
        database.connection().execute("INSERT INTO t (id, name) VALUES (4, 'd')");

        // The offset persisted by the previous stop means restart resumes streaming directly rather
        // than re-running the snapshot.
        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        List<SourceRecord> afterRestart = consumeRecordsByTopic(1, false).recordsForTopic(TOPIC_PREFIX + ".t");
        assertThat(afterRestart).hasSize(1);

        SourceRecord resumed = afterRestart.get(0);
        assertThat(operation(resumed)).isEqualTo(Envelope.Operation.CREATE.code());
        assertThat(((Struct) resumed.key()).getInt64("id")).isEqualTo(4L);
        assertThat(source(resumed).getInt64("change_id")).isEqualTo(3L);
    }

    private static String operation(SourceRecord record) {
        return ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
    }

    private static Struct source(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
    }
}
