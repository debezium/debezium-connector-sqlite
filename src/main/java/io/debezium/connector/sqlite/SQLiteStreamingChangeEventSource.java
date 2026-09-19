/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import io.debezium.relational.Tables;
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

    // Shape changes wait here until streaming drains past the change_id at which the schema changed, so a
    // backlogged row is still rendered against the shape that captured it rather than the newer one.
    private final List<PendingSwap> pendingSwaps = new ArrayList<>();

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
     * Reconciles the capture triggers when {@code schema_version} has moved since the last check, picking
     * up a schema change made while streaming. The first poll always reconciles, catching a change made
     * during the snapshot; an irrelevant bump such as {@code CREATE INDEX} reconciles to a no-op.
     */
    private void reconcileIfSchemaChanged() {
        long current = readSchemaVersion();
        if (lastSchemaVersion == null || current != lastSchemaVersion) {
            if (lastSchemaVersion != null) {
                LOGGER.debug("SQLite schema_version changed from {} to {}; reconciling capture triggers",
                        lastSchemaVersion, current);
            }
            reconcileNow();
        }
    }

    private void reconcileNow() {
        try {
            Map<TableId, Table> emitted = currentShapes();
            TriggerReconciler.reconcile(connection, config.getTableFilters().dataCollectionFilter());
            // Every row up to here was captured under the old triggers; rows after it under the rebuilt ones.
            long boundary = connection.readMaxChangeId();
            deferShapeChanges(emitted, schema.readDatabaseTables(connection), boundary);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to reconcile capture triggers after a schema change", e);
        }
        // The reconcile's own DROP/CREATE TRIGGER statements bump schema_version, so read it back
        // afterward. Recording the pre-reconcile value would leave lastSchemaVersion stale and force a
        // redundant no-op reconcile on the next poll.
        lastSchemaVersion = readSchemaVersion();
    }

    /**
     * Reconciles the emitted schema with the database after a schema change, deferring the shapes a
     * backlog still depends on. A table added since the last reconcile is registered now, because every
     * row that can name it was captured after {@code boundary}. A table whose columns changed, or that was
     * dropped, keeps its old shape registered and records a swap to apply once streaming passes the
     * boundary, so rows up to it still render against the shape that captured them.
     */
    private void deferShapeChanges(Map<TableId, Table> emitted, Tables database, long boundary) {
        for (TableId id : database.tableIds()) {
            Table current = database.forTable(id);
            Table previous = emitted.get(id);
            if (previous == null) {
                schema.registerTable(current);
            }
            else if (!previous.retrieveColumnNames().equals(current.retrieveColumnNames())) {
                pendingSwaps.add(new PendingSwap(boundary, id, current));
            }
        }
        for (TableId id : emitted.keySet()) {
            if (!database.tableIds().contains(id)) {
                pendingSwaps.add(new PendingSwap(boundary, id, null));
            }
        }
    }

    /**
     * Applies every deferred swap whose boundary this change_id has passed, so a row is rendered against
     * the shape in effect at its position: the old shape up to the boundary, the new shape after it. A
     * swap with a null shape drops a table the database no longer has.
     */
    private void applyDueSwaps(long changeId) {
        Iterator<PendingSwap> swaps = pendingSwaps.iterator();
        while (swaps.hasNext()) {
            PendingSwap swap = swaps.next();
            if (changeId > swap.boundary) {
                if (swap.newShape == null) {
                    schema.evictTable(swap.tableId);
                }
                else {
                    schema.registerTable(swap.newShape);
                }
                swaps.remove();
            }
        }
    }

    private Map<TableId, Table> currentShapes() {
        Map<TableId, Table> shapes = new HashMap<>();
        for (TableId id : schema.tableIds()) {
            shapes.put(id, schema.tableFor(id));
        }
        return shapes;
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
        // Adopt any schema change this row has reached before rendering it against the emitted shape.
        applyDueSwaps(row.changeId());
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
     * does not have yet when a {@code CREATE} or {@code RENAME} this poll has not caught, so it reconciles
     * once if the schema has moved and looks again. Empty means the table is gone and the caller skips it.
     */
    private Optional<TableId> resolveTable(String tableName) {
        Optional<TableId> found = findTable(tableName);
        if (found.isPresent()) {
            return found;
        }
        long current = readSchemaVersion();
        if (lastSchemaVersion == null || current != lastSchemaVersion) {
            reconcileNow();
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

    /** A schema change held until streaming passes {@code boundary}. A null {@code newShape} drops the table. */
    private static final class PendingSwap {

        private final long boundary;
        private final TableId tableId;
        private final Table newShape;

        PendingSwap(long boundary, TableId tableId, Table newShape) {
            this.boundary = boundary;
            this.tableId = tableId;
            this.newShape = newShape;
        }
    }
}
