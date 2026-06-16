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
import io.debezium.snapshot.SnapshotterService;

/**
 * Performs the initial snapshot of the SQLite data source.
 *
 * <p>This is a stub: it makes the snapshot decision and threads the offset through, but does not yet
 * read any table rows. The snapshot decision is delegated to the {@link SnapshotterService} resolved
 * from {@code snapshot.mode}, matching how the other Debezium connectors decide.
 */
class SQLiteSnapshotChangeEventSource
        implements SnapshotChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteSnapshotChangeEventSource.class);

    private final SQLiteConnectorConfig config;
    private final SnapshotterService snapshotterService;

    SQLiteSnapshotChangeEventSource(SQLiteConnectorConfig config, SnapshotterService snapshotterService) {
        this.config = config;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(SQLitePartition partition, SQLiteOffsetContext offsetContext) {
        boolean offsetExists = offsetContext != null;
        boolean shouldSnapshot = snapshotterService.getSnapshotter().shouldSnapshotData(offsetExists, false);
        LOGGER.info("Snapshot decision: shouldSnapshot={}", shouldSnapshot);
        return new SnapshottingTask(shouldSnapshot, shouldSnapshot, List.of(), Map.of(), false);
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
        // On a first run there is no stored offset, so start from the beginning of the CDC log.
        SQLiteOffsetContext effectiveOffset = offsetContext != null ? offsetContext : SQLiteOffsetContext.initial(config);

        if (task.shouldSkipSnapshot()) {
            LOGGER.info("Skipping snapshot, resuming streaming from change_id {}", effectiveOffset.getChangeId());
            return SnapshotResult.skipped(effectiveOffset);
        }

        // The data snapshot is not implemented yet; this stub completes without reading any rows.
        LOGGER.info("Starting SQLite snapshot");
        LOGGER.info("SQLite snapshot complete");
        return SnapshotResult.completed(effectiveOffset);
    }
}
