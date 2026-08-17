/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    /** The {@code schema_version} seen at the last reconcile; null until the first poll seeds it. */
    private Long lastSchemaVersion;

    /**
     * The {@code change_id} Kafka Connect has most recently confirmed committed, read by
     * {@link #commitOffset} on the commit thread and read by the poll loop on the streaming thread; 0
     * until the first commit lands.
     */
    private volatile long committedChangeId;

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
        // The snapshot leaves the shared connection in manual-commit mode to hold its read view. Switch
        // to autocommit so each poll is a fresh short read that sees new commits and lets SQLite
        // checkpoint the WAL between polls.
        enterAutocommit();
        Metronome metronome = Metronome.sleeper(config.getPollInterval(), clock);

        while (context.isRunning()) {
            reconcileIfSchemaChanged();
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

    /**
     * Reconciles the capture triggers when {@code schema_version} has moved since the last check, so a
     * schema change made while streaming is picked up before the next batch. The first poll always
     * reconciles, which also catches a change made between startup and the start of streaming, such as
     * during the snapshot. A bump with no relevant change, for example a {@code CREATE INDEX}, reconciles
     * to a no-op.
     */
    private void reconcileIfSchemaChanged() {
        long current = readSchemaVersion();
        if (lastSchemaVersion == null || current != lastSchemaVersion) {
            if (lastSchemaVersion != null) {
                LOGGER.debug("SQLite schema_version changed from {} to {}; reconciling capture triggers",
                        lastSchemaVersion, current);
            }
            reconcileNow(current);
        }
    }

    private void reconcileNow(long schemaVersion) {
        try {
            schema.refresh(connection);
            TriggerReconciler.reconcile(connection, schema);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to reconcile capture triggers after a schema change", e);
        }
        lastSchemaVersion = schemaVersion;
    }

    private long readSchemaVersion() {
        try {
            return connection.readSchemaVersion();
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read the SQLite schema_version", e);
        }
    }

    private void enterAutocommit() {
        try {
            connection.setAutoCommit(true);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to switch the streaming connection to autocommit", e);
        }
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
        // Advance the offset first so a skipped row is not read again on the next poll.
        effectiveOffset.setChangeId(row.changeId());
        Optional<TableId> tableId = resolveTable(row.tableName());
        if (tableId.isEmpty()) {
            LOGGER.warn("Skipping change {} for table '{}' that is not monitored; it was likely renamed or dropped",
                    row.changeId(), row.tableName());
            return;
        }
        Table table = schema.tableFor(tableId.get());
        effectiveOffset.event(tableId.get(), Instant.ofEpochMilli(row.committedAt()));
        SQLiteChangeRecordEmitter emitter = new SQLiteChangeRecordEmitter(partition, effectiveOffset,
                SQLiteChangeRecordEmitter.operationFor(row.operation()), table,
                row.oldRowData(), row.newRowData(), clock, config);
        dispatcher.dispatchDataChangeEvent(partition, tableId.get(), emitter);
    }

    /**
     * Resolves a change row's table name to a {@link TableId}. A row can name a table the loaded schema
     * does not have when a {@code CREATE} or {@code RENAME} happened that this poll has not caught yet, so
     * it reconciles once, if the schema has moved since the last reconcile, and looks again. An empty
     * result means the table is gone, renamed away or dropped, and the caller skips the row.
     */
    private Optional<TableId> resolveTable(String tableName) {
        Optional<TableId> found = findTable(tableName);
        if (found.isPresent()) {
            return found;
        }
        long current = readSchemaVersion();
        if (lastSchemaVersion == null || current != lastSchemaVersion) {
            reconcileNow(current);
            found = findTable(tableName);
        }
        return found;
    }

    private Optional<TableId> findTable(String tableName) {
        return schema.tableIds().stream()
                .filter(id -> tableName.equals(id.table()))
                .findFirst();
    }

    @Override
    public SQLiteOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    /**
     * Records the {@code change_id} Kafka Connect has durably committed, so the poll loop knows how far
     * it may compact the log. Called on the commit thread, a different thread from the one running
     * {@link #execute}, so this does no database work of its own; it only stores a volatile field for
     * the poll loop to read.
     */
    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        committedChangeId = ((Number) offset.get(SQLiteOffsetContext.CHANGE_ID_KEY)).longValue();
    }
}
