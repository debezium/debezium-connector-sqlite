/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Streams ongoing changes from the SQLite {@code _debezium_cdc_log} table.
 *
 * <p>Implement {@link #execute} in Phase 1 to poll new rows from {@code _debezium_cdc_log}
 * and dispatch them via the {@link io.debezium.pipeline.EventDispatcher}. The loop must
 * check {@link ChangeEventSourceContext#isRunning()} and exit cleanly when it returns
 * {@code false}.
 */
class SQLiteStreamingChangeEventSource
        implements StreamingChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteStreamingChangeEventSource.class);

    private final SQLiteConnectorConfig config;

    SQLiteStreamingChangeEventSource(SQLiteConnectorConfig config) {
        this.config = config;
    }

    @Override
    public void execute(ChangeEventSourceContext context,
                        SQLitePartition partition,
                        SQLiteOffsetContext offsetContext)
            throws InterruptedException {
        LOGGER.info("Starting SQLite streaming from change_id {}", offsetContext.getChangeId());

        while (context.isRunning()) {
            // TODO: poll _debezium_cdc_log for rows with change_id > offsetContext.getChangeId()
            // and dispatch each one via the EventDispatcher. Implemented in Phase 1.
            Thread.sleep(1_000);
        }

        LOGGER.info("SQLite streaming stopped");
    }
}
