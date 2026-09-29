/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Streaming metrics for the SQLite connector. Adds the connector's own position and backlog to the
 * default streaming metrics: the current and committed {@code change_id} and the CDC log depth. The
 * streaming source records these; the JMX thread only reads them, so they are held in atomic fields
 * and reading them never touches the database.
 */
@ThreadSafe
public class SQLiteStreamingChangeEventSourceMetrics
        extends DefaultStreamingChangeEventSourceMetrics<SQLitePartition>
        implements SQLiteStreamingChangeEventSourceMetricsMXBean {

    private final AtomicLong currentChangeId = new AtomicLong(0);
    private final AtomicLong committedChangeId = new AtomicLong(0);
    private final AtomicLong cdcLogDepth = new AtomicLong(0);

    public <T extends CdcSourceTaskContext> SQLiteStreamingChangeEventSourceMetrics(T taskContext,
                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider,
                                                                                    CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }

    /** Records the {@code change_id} of the row just dispatched. */
    public void setCurrentChangeId(long changeId) {
        currentChangeId.set(changeId);
    }

    /** Records the {@code change_id} Kafka Connect just committed. */
    public void setCommittedChangeId(long changeId) {
        committedChangeId.set(changeId);
    }

    /** Records the current CDC log depth, the backlog not yet compacted. */
    public void setCdcLogDepth(long depth) {
        cdcLogDepth.set(depth);
    }

    @Override
    public long getCurrentChangeId() {
        return currentChangeId.get();
    }

    @Override
    public long getCommittedChangeId() {
        return committedChangeId.get();
    }

    @Override
    public long getCdcLogDepth() {
        return cdcLogDepth.get();
    }

    @Override
    public void reset() {
        super.reset();
        currentChangeId.set(0);
        committedChangeId.set(0);
        cdcLogDepth.set(0);
    }
}
