/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the current read position within the SQLite CDC log.
 *
 * <p>The offset map returned by {@link #getOffset()} is persisted by the Kafka Connect
 * framework and used to resume after a restart. The position is the last {@code change_id}
 * value consumed from the {@code _debezium_cdc_log} table.
 */
public class SQLiteOffsetContext extends CommonOffsetContext<SQLiteSourceInfo> {

    static final String CHANGE_ID_KEY = "change_id";

    private long changeId;

    public SQLiteOffsetContext(SQLiteSourceInfo sourceInfo) {
        super(sourceInfo);
    }

    /** Returns the last {@code change_id} consumed from {@code _debezium_cdc_log}. */
    public long getChangeId() {
        return changeId;
    }

    /** Advances the position to the given {@code change_id}. */
    public void setChangeId(long changeId) {
        this.changeId = changeId;
    }

    @Override
    public Map<String, ?> getOffset() {
        return Map.of(CHANGE_ID_KEY, changeId);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        // Nothing to update yet; timestamp is read live from Instant.now() in SQLiteSourceInfo.
    }

    @Override
    public TransactionContext getTransactionContext() {
        return new TransactionContext();
    }
}
