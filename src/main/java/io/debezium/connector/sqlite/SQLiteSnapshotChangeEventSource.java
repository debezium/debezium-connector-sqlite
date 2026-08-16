/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Performs the initial snapshot, emitting a read ({@code r}) record per row of every monitored table.
 * The {@link RelationalSnapshotChangeEventSource base class} drives it inside one WAL read
 * transaction. Reading the high-water mark in {@link #determineSnapshotOffset} as the first statement
 * opens a consistent view, so any change logged after it is left for streaming with no gap or duplicate.
 */
class SQLiteSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<SQLitePartition, SQLiteOffsetContext> {

    private final SQLiteConnectorConfig connectorConfig;
    private final SQLiteConnection jdbcConnection;
    private final SQLiteDatabaseSchema schema;

    SQLiteSnapshotChangeEventSource(SQLiteConnectorConfig connectorConfig, SnapshotterService snapshotterService,
                                    MainConnectionProvidingConnectionFactory<SQLiteConnection> connectionFactory, SQLiteDatabaseSchema schema,
                                    EventDispatcher<SQLitePartition, TableId> dispatcher, Clock clock,
                                    SnapshotProgressListener<SQLitePartition> snapshotProgressListener,
                                    NotificationService<SQLitePartition, SQLiteOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.schema = schema;
    }

    @Override
    protected SnapshotContext<SQLitePartition, SQLiteOffsetContext> prepare(SQLitePartition partition, boolean onDemand) {
        // SQLite has no catalog, so the catalog name is null.
        return new SQLiteSnapshotContext(partition, null, onDemand);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext)
            throws SQLException {
        // The base filters this through the system-tables predicate, dropping sqlite_* and the
        // connector's own _debezium_ tables. SQLite has no catalog, so the catalog name is null.
        return jdbcConnection.getAllTableIds(null);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext) {
        // No locking. The snapshot runs in a WAL read transaction, which gives a consistent view
        // without blocking writers.
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext,
                                           SQLiteOffsetContext previousOffset)
            throws SQLException {
        SQLiteOffsetContext offset = previousOffset != null ? previousOffset : SQLiteOffsetContext.initial(connectorConfig);
        // Reads the high-water mark inside the snapshot connection's frozen WAL view. Every snapshot
        // read shares that one view, so the mark and the data are consistent and any change logged
        // after it is left for streaming with no gap or duplicate.
        offset.setChangeId(jdbcConnection.readMaxChangeId());
        snapshotContext.offset = offset;
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext, SQLiteOffsetContext offsetContext,
                                      SnapshottingTask snapshottingTask)
            throws SQLException {
        // Populate the snapshot context's table models so the data read can resolve each row's columns.
        // The data collection filter keeps the system tables out.
        jdbcConnection.readSchema(snapshotContext.tables, snapshotContext.catalogName, null,
                connectorConfig.getTableFilters().dataCollectionFilter(), null, false);
        // Load the same tables into the connector schema so the dispatcher can build Kafka schemas.
        schema.refresh(jdbcConnection);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext) {
        // Nothing to release; no locks are taken.
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext, Table table) {
        // Required by the base, but never invoked: the SQLite schema is not historized, so the base
        // skips persisting schema history.
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return snapshotterService.getSnapshotQuery().snapshotQuery(tableId.toDoubleQuotedString(), columns);
    }

    @Override
    protected SQLiteOffsetContext copyOffset(RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> snapshotContext) {
        return new SQLiteOffsetLoader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

    private static class SQLiteSnapshotContext extends RelationalSnapshotContext<SQLitePartition, SQLiteOffsetContext> {

        SQLiteSnapshotContext(SQLitePartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }
}
