/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset change id, the position in the {@code _debezium_cdc_log} table, is compared
 * against the value captured when the monitor was last consulted, and when the position has
 * not moved, a warning is logged.
 * <p>
 * No check is performed until the first change has been consumed, so a connector that has
 * not yet read its first change log row is not reported as stale.
 *
 * @author Chris Cranford
 */
public class SQLiteOffsetActivityMonitor implements OffsetActivityMonitor<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteOffsetActivityMonitor.class);

    private final Duration checkInterval;

    private Long previousChangeId;

    public SQLiteOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(SQLitePartition partition, SQLiteOffsetContext offsetContext) {
        final long changeId = offsetContext.getChangeId();

        // Check for stale state
        if (changeId > 0 && Objects.equals(previousChangeId, changeId)) {
            LOGGER.warn("Offset change id {} has not changed in at least {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that changes are no longer being written to the change log table.",
                    changeId, checkInterval.toMillis());
        }

        // Update tracked stats
        previousChangeId = changeId;
    }

}