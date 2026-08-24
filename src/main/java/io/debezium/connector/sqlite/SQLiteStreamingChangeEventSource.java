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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Streams ongoing changes from the SQLite {@code _debezium_cdc_log} table. {@link #execute} runs a poll
 * loop: read the next batch after the resume position, dispatch each row, advance the offset per row. A
 * full batch polls again at once so a backlog drains; an empty poll sleeps for {@code poll.interval.ms}.
 */
class SQLiteStreamingChangeEventSource
        implements StreamingChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteStreamingChangeEventSource.class);

    private final SQLiteConnectorConfig config;
    private final SQLiteConnection connection;
    private final SQLiteDatabaseSchema schema;
    private final EventDispatcher<SQLitePartition, TableId> dispatcher;
    private final Clock clock;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private OffsetActivityMonitor<SQLitePartition, SQLiteOffsetContext> offsetActivityMonitor;

    private SQLiteOffsetContext effectiveOffset;

    private Long lastSchemaVersion;

    /**
     * The {@code change_id} Kafka Connect has most recently confirmed committed, read by
     * {@link #commitOffset} on the commit thread and read by the poll loop on the streaming thread; 0
     * until the first commit lands.
     */
    private volatile long committedChangeId;

    /** The {@code change_id} the log was last compacted up to; 0 until the first compaction. */
    private long lastCompactedChangeId;

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
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(config.getServiceRegistry());
    }

    /**
     * Loads the schema and sets the resume point. A null offset (nothing stored, no snapshot, as with
     * {@code snapshot.mode=no_data} on a first start) begins streaming at the log end.
     */
    @Override
    public void init(SQLiteOffsetContext offsetContext) {
        // The snapshot-skipped path has not loaded the schema.
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
        // The snapshot left the connection in manual-commit mode. Autocommit makes each poll a fresh
        // short read that sees new commits and lets SQLite checkpoint the WAL between polls.
        enterAutocommit();
        Metronome metronome = Metronome.sleeper(config.getPollInterval(), clock);

        while (context.isRunning()) {
            offsetActivityMonitorService.pulse(partition, offsetContext);
            reconcileIfSchemaChanged(partition);
            compactIfNeeded();
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
     * Reconciles the capture triggers when {@code schema_version} has moved since the last check, picking
     * up a schema change made while streaming. The first poll always reconciles, catching a change made
     * during the snapshot; an irrelevant bump such as {@code CREATE INDEX} reconciles to a no-op.
     */
    private void reconcileIfSchemaChanged(SQLitePartition partition) throws InterruptedException {
        long current = readSchemaVersion();
        if (lastSchemaVersion == null || current != lastSchemaVersion) {
            if (lastSchemaVersion != null) {
                LOGGER.debug("SQLite schema_version changed from {} to {}; reconciling capture triggers",
                        lastSchemaVersion, current);
            }
            reconcileNow(partition, current);
        }
    }

    private void reconcileNow(SQLitePartition partition, long schemaVersion) throws InterruptedException {
        Map<String, Table> tablesBeforeRefresh = tablesByName();
        ReconcileResult result;
        try {
            schema.refresh(connection);
            result = TriggerReconciler.reconcile(connection, schema, tablesBeforeRefresh.keySet());
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to reconcile capture triggers after a schema change", e);
        }
        lastSchemaVersion = schemaVersion;
        dispatchSchemaChangeEvents(partition, result, tablesBeforeRefresh);
    }

    private Map<String, Table> tablesByName() {
        return schema.tableIds().stream().collect(Collectors.toMap(TableId::table, schema::tableFor));
    }

    /**
     * Announces each table the reconcile touched: a created or altered table dispatches with its
     * current shape, a dropped table dispatches with the shape it had just before the refresh removed
     * it. No literal DDL is ever available, so {@code ddl} is always null.
     */
    private void dispatchSchemaChangeEvents(SQLitePartition partition, ReconcileResult result, Map<String, Table> tablesBeforeRefresh)
            throws InterruptedException {
        for (String table : result.created()) {
            TableId tableId = findTable(table).orElseThrow();
            dispatchSchemaChangeEvent(partition, tableId,
                    SchemaChangeEvent.ofCreate(partition, effectiveOffset, config.getLogicalName(), null, null, schema.tableFor(tableId), false));
        }
        for (String table : result.altered()) {
            TableId tableId = findTable(table).orElseThrow();
            dispatchSchemaChangeEvent(partition, tableId,
                    SchemaChangeEvent.ofAlter(partition, effectiveOffset, config.getLogicalName(), null, null, schema.tableFor(tableId)));
        }
        for (String table : result.dropped()) {
            Table droppedTable = tablesBeforeRefresh.get(table);
            dispatchSchemaChangeEvent(partition, droppedTable.id(),
                    SchemaChangeEvent.ofDrop(partition, effectiveOffset, config.getLogicalName(), null, null, droppedTable));
        }
    }

    private void dispatchSchemaChangeEvent(SQLitePartition partition, TableId tableId, SchemaChangeEvent event) throws InterruptedException {
        dispatcher.dispatchSchemaChangeEvent(partition, effectiveOffset, tableId, (receiver) -> {
            try {
                receiver.schemaChangeEvent(event);
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        });
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

    /**
     * Deletes already-committed rows once enough of them have accumulated since the last delete. The
     * bound is {@link #committedChangeId}, never the offset the poll loop is dispatching, so a row that
     * has only been dispatched and not yet offset-committed is never deleted.
     */
    private void compactIfNeeded() {
        long committed = committedChangeId;
        if (committed - lastCompactedChangeId < config.getLogCompactionThreshold()) {
            return;
        }
        try {
            connection.deleteChangesUpTo(committed);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to compact " + CdcLog.TABLE_NAME, e);
        }
        lastCompactedChangeId = committed;
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
        Optional<TableId> tableId = resolveTable(partition, row.tableName());
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
     * does not have yet when a {@code CREATE} or {@code RENAME} this poll has not caught, so it reconciles
     * once if the schema has moved and looks again. Empty means the table is gone and the caller skips it.
     */
    private Optional<TableId> resolveTable(SQLitePartition partition, String tableName) throws InterruptedException {
        Optional<TableId> found = findTable(tableName);
        if (found.isPresent()) {
            return found;
        }
        long current = readSchemaVersion();
        if (lastSchemaVersion == null || current != lastSchemaVersion) {
            reconcileNow(partition, current);
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

    @Override
    public Optional<OffsetActivityMonitor<SQLitePartition, SQLiteOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new SQLiteOffsetActivityMonitor(config.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
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
