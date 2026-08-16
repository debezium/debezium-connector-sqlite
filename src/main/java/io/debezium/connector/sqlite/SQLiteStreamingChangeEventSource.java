/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
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
    private final SQLiteConnection connection;
    private final SQLiteDatabaseSchema schema;

    SQLiteStreamingChangeEventSource(SQLiteConnectorConfig config, SQLiteConnection connection, SQLiteDatabaseSchema schema) {
        this.config = config;
        this.connection = connection;
        this.schema = schema;
    }

    @Override
    public void init(SQLiteOffsetContext offsetContext) {
        // Load the schema for the case where the snapshot was skipped and did not load it.
        try {
            schema.refresh(connection);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to load the SQLite schema", e);
        }
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
