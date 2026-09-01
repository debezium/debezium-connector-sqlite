/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the read position in the SQLite CDC log as the last {@code change_id} consumed. The map from
 * {@link #getOffset()} is persisted by Kafka Connect and used to resume after a restart.
 */
public class SQLiteOffsetContext extends CommonOffsetContext<SQLiteSourceInfo> {

    static final String CHANGE_ID_KEY = "change_id";

    private long changeId;

    public SQLiteOffsetContext(SQLiteSourceInfo sourceInfo) {
        super(sourceInfo);
    }

    /** Restores an offset from persisted state, including the snapshot markers, so a restart resumes in the same phase. */
    public SQLiteOffsetContext(SQLiteSourceInfo sourceInfo, long changeId, SnapshotType snapshot, boolean snapshotCompleted) {
        super(sourceInfo, snapshotCompleted);
        this.changeId = changeId;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    /** A fresh offset for a connector that has never run, positioned at {@code change_id} 0. */
    public static SQLiteOffsetContext initial(SQLiteConnectorConfig config) {
        return new SQLiteOffsetContext(new SQLiteSourceInfo(config));
    }

    public long getChangeId() {
        return changeId;
    }

    public void setChangeId(long changeId) {
        this.changeId = changeId;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        if (getSnapshot().isPresent()) {
            offset.put(AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        }
        offset.put(CHANGE_ID_KEY, changeId);
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        sourceInfo.setChange((TableId) dataCollectionId, instant, changeId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return new TransactionContext();
    }
}
