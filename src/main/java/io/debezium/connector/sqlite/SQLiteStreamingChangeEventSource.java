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
 * Streams ongoing changes from the SQLite {@code _debezium_cdc_log} table. This is a stub: {@link
 * #execute} runs the poll loop but does not read or dispatch rows yet.
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
            // and dispatch each one via the EventDispatcher.
            Thread.sleep(1_000);
        }

        LOGGER.info("SQLite streaming stopped");
    }
}
