/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;

/**
 * Unit tests for the snapshot markers {@link SQLiteOffsetContext} carries while the relational
 * snapshot base drives the snapshot lifecycle. The base calls {@code preSnapshotStart},
 * {@code preSnapshotCompletion}, and {@code postSnapshotCompletion} on the offset, and the offset
 * must report that state so a snapshot record's {@code source} shows {@code snapshot: true} and the
 * post-snapshot offset no longer says it is snapshotting.
 */
class SQLiteOffsetContextTest {

    private static SQLiteOffsetContext newOffset() {
        Configuration config = Configuration.from(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "test.db",
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test"));
        return SQLiteOffsetContext.initial(new SQLiteConnectorConfig(config));
    }

    @Test
    void freshOffsetHasNoSnapshotState() {
        Map<String, ?> offset = newOffset().getOffset();

        assertThat(offset.get(SQLiteOffsetContext.CHANGE_ID_KEY)).isEqualTo(0L);
        assertThat(offset).doesNotContainKey(AbstractSourceInfo.SNAPSHOT_KEY);
        assertThat(offset).doesNotContainKey(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY);
    }

    @Test
    void snapshotStartMarksOffsetAndSource() {
        SQLiteOffsetContext offset = newOffset();

        offset.preSnapshotStart(false);

        // The source struct renders this marker into its snapshot field.
        assertThat(offset.getSourceInfo().getString("snapshot")).isEqualTo("true");
        Map<String, ?> map = offset.getOffset();
        assertThat(map.get(AbstractSourceInfo.SNAPSHOT_KEY)).isEqualTo("INITIAL");
        assertThat(map.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);
    }

    @Test
    void preSnapshotCompletionSetsCompletedFlag() {
        SQLiteOffsetContext offset = newOffset();

        offset.preSnapshotStart(false);
        offset.preSnapshotCompletion();

        assertThat(offset.getOffset().get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);
    }

    @Test
    void postSnapshotCompletionClearsSnapshotState() {
        SQLiteOffsetContext offset = newOffset();

        offset.preSnapshotStart(false);
        offset.preSnapshotCompletion();
        offset.postSnapshotCompletion();

        Map<String, ?> map = offset.getOffset();
        assertThat(map).doesNotContainKey(AbstractSourceInfo.SNAPSHOT_KEY);
        assertThat(map).doesNotContainKey(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY);
        assertThat(map.get(SQLiteOffsetContext.CHANGE_ID_KEY)).isEqualTo(0L);
    }

    @Test
    void snapshotMarkerOnSourceInfoFlipsFromTrueToFalse() {
        SQLiteOffsetContext offset = newOffset();

        offset.preSnapshotStart(false);
        offset.markSnapshotRecord(SnapshotRecord.LAST);
        assertThat(offset.getSourceInfo().getString("snapshot")).isEqualTo("last");

        offset.postSnapshotCompletion();
        assertThat(offset.getSourceInfo().getString("snapshot")).isNull();
    }
}
