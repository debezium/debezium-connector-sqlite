/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Streams ongoing changes from the SQLite {@code _debezium_cdc_log} table.
 *
 * <p>A stub: it establishes the resume point and idles until the task stops, without reading the log.
 */
class SQLiteStreamingChangeEventSource
        implements StreamingChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteStreamingChangeEventSource.class);

    private static final long IDLE_SLEEP_MS = 1_000;

    private final SQLiteConnectorConfig config;
    private final SQLiteConnection connection;
    private final SQLiteDatabaseSchema schema;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private OffsetActivityMonitor<SQLitePartition, SQLiteOffsetContext> offsetActivityMonitor;

    private SQLiteOffsetContext effectiveOffset;

    SQLiteStreamingChangeEventSource(SQLiteConnectorConfig config, SQLiteConnection connection, SQLiteDatabaseSchema schema) {
        this.config = config;
        this.connection = connection;
        this.schema = schema;
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(config.getServiceRegistry());
    }

    /**
     * Loads the schema and sets the resume point. The offset is null when nothing was stored and no
     * snapshot ran, as with {@code snapshot.mode=no_data} on a first start; streaming then begins at
     * the end of the log rather than replaying it.
     */
    @Override
    public void init(SQLiteOffsetContext offsetContext) {
        // Load the schema for the case where the snapshot was skipped and did not load it.
        try {
            schema.refresh(connection);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to load the SQLite schema", e);
        }

        if (offsetContext != null) {
            effectiveOffset = offsetContext;
            return;
        }

        effectiveOffset = SQLiteOffsetContext.initial(config);
        effectiveOffset.setChangeId(connection.readMaxChangeId());
    }

    @Override
    public void execute(ChangeEventSourceContext context,
                        SQLitePartition partition,
                        SQLiteOffsetContext offsetContext)
            throws InterruptedException {
        LOGGER.info("Starting SQLite streaming from change_id {}", effectiveOffset.getChangeId());

        while (context.isRunning()) {
            Thread.sleep(IDLE_SLEEP_MS);

            offsetActivityMonitorService.pulse(partition, offsetContext);
        }

        LOGGER.info("SQLite streaming stopped");
    }

    @Override
    public SQLiteOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    @Override
    public Optional<OffsetActivityMonitor<SQLitePartition, SQLiteOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new SQLiteOffsetActivityMonitor(config.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
    }
}
