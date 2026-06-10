/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * Performs an initial snapshot of the SQLite data source.
 *
 * <p>Override {@link #execute} in Phase 1 to read existing table rows via JDBC and emit READ
 * events through the {@link io.debezium.pipeline.EventDispatcher}. When the snapshot is done,
 * update the offset context so streaming resumes at the correct {@code change_id}.
 */
class SQLiteSnapshotChangeEventSource
        implements SnapshotChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteSnapshotChangeEventSource.class);

    private final SQLiteConnectorConfig config;

    SQLiteSnapshotChangeEventSource(SQLiteConnectorConfig config) {
        this.config = config;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(SQLitePartition partition, SQLiteOffsetContext offsetContext) {
        boolean shouldSnapshot = config.getSnapshotMode().shouldSnapshotData(
                offsetContext.getChangeId() != 0, false);
        LOGGER.info("Snapshot decision: shouldSnapshot={}", shouldSnapshot);
        return new SnapshottingTask(shouldSnapshot, false, List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(SQLitePartition partition,
                                                        SQLiteOffsetContext offsetContext,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return getSnapshottingTask(partition, offsetContext);
    }

    @Override
    public SnapshotResult<SQLiteOffsetContext> execute(ChangeEventSourceContext context,
                                                       SQLitePartition partition,
                                                       SQLiteOffsetContext offsetContext,
                                                       SnapshottingTask task)
            throws InterruptedException {
        if (task.shouldSkipSnapshot()) {
            LOGGER.info("Skipping snapshot — resuming streaming from change_id {}", offsetContext.getChangeId());
            return SnapshotResult.skipped(offsetContext);
        }

        LOGGER.info("Starting SQLite snapshot");
        // TODO: implement snapshot in Phase 1.
        LOGGER.info("SQLite snapshot complete");
        return SnapshotResult.completed(offsetContext);
    }
}
