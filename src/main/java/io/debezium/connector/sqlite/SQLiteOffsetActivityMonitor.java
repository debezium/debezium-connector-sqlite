/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Duration;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset change id, the position in the {@code _debezium_cdc_log} table, is compared
 * against the value captured when the monitor was last consulted, and when the position has
 * not moved, a stale result is reported.
 * <p>
 * No check is performed until the first change has been consumed, so a connector that has
 * not yet read its first change log row is not reported as stale.
 *
 * @author Chris Cranford
 */
public class SQLiteOffsetActivityMonitor implements OffsetActivityMonitor<SQLitePartition, SQLiteOffsetContext> {

    private final Duration checkInterval;

    private Long previousChangeId;

    public SQLiteOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(SQLitePartition partition, SQLiteOffsetContext offsetContext) {
        final long changeId = offsetContext.getChangeId();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (changeId > 0 && Objects.equals(previousChangeId, changeId)) {
            result = StaleOffsetsResult.stale(
                    ("Offset change id %d has not changed in at least %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that changes are no longer being written to the change log table.")
                            .formatted(changeId, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousChangeId = changeId;

        return result;
    }

}