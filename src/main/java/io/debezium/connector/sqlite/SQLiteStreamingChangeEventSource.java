/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Streams ongoing changes from the SQLite {@code _debezium_cdc_log} table.
 *
 * <p>{@link #execute} runs a bounded poll loop: it reads the next batch of rows after the resume
 * position, dispatches each as a change event, and advances the offset after each row. A full batch is
 * followed immediately by the next poll so a backlog drains without delay; an empty poll sleeps for
 * {@code poll.interval.ms} so an idle connector does not spin. Each poll is a short read, so it does not
 * hold a read transaction across the sleep and block WAL checkpointing.
 */
class SQLiteStreamingChangeEventSource
        implements StreamingChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteStreamingChangeEventSource.class);

    private final SQLiteConnectorConfig config;
    private final SQLiteConnection connection;
    private final SQLiteDatabaseSchema schema;
    private final EventDispatcher<SQLitePartition, TableId> dispatcher;
    private final Clock clock;

    private SQLiteOffsetContext effectiveOffset;

    SQLiteStreamingChangeEventSource(SQLiteConnectorConfig config,
                                     SQLiteConnection connection,
                                     SQLiteDatabaseSchema schema,
                                     EventDispatcher<SQLitePartition, TableId> dispatcher,
                                     Clock clock) {
        this.config = config;
        this.connection = connection;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
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
        Metronome metronome = Metronome.sleeper(config.getPollInterval(), clock);

        while (context.isRunning()) {
            List<CdcLogRow> batch = readBatch();
            if (batch.isEmpty()) {
                metronome.pause();
                continue;
            }
            for (CdcLogRow row : batch) {
                dispatch(partition, row);
            }
        }

        LOGGER.info("SQLite streaming stopped");
    }

    private List<CdcLogRow> readBatch() {
        try {
            return connection.readChanges(effectiveOffset.getChangeId(), config.getCdcLogBatchSize());
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read changes from " + CdcLog.TABLE_NAME, e);
        }
    }

    private void dispatch(SQLitePartition partition, CdcLogRow row) throws InterruptedException {
        TableId tableId = tableIdFor(row.tableName());
        Table table = schema.tableFor(tableId);
        // Advance the offset before dispatch so the enqueued record carries this row as its resume point.
        effectiveOffset.setChangeId(row.changeId());
        effectiveOffset.event(tableId, Instant.ofEpochMilli(row.committedAt()));
        SQLiteChangeRecordEmitter emitter = new SQLiteChangeRecordEmitter(partition, effectiveOffset,
                SQLiteChangeRecordEmitter.operationFor(row.operation()), table,
                row.oldRowData(), row.newRowData(), clock, config);
        dispatcher.dispatchDataChangeEvent(partition, tableId, emitter);
    }

    private TableId tableIdFor(String tableName) {
        return schema.tableIds().stream()
                .filter(id -> tableName.equals(id.table()))
                .findFirst()
                .orElseThrow(() -> new DebeziumException("Streaming encountered a change for table '" + tableName
                        + "' that is not in the schema; schema changes during streaming are not yet supported."));
    }

    @Override
    public SQLiteOffsetContext getOffsetContext() {
        return effectiveOffset;
    }
}
