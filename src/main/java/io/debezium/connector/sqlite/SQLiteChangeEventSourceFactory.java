/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Creates the snapshot and streaming change event sources for the SQLite connector.
 *
 * <p>Called once per connector start by
 * {@link io.debezium.pipeline.ChangeEventSourceCoordinator}.
 */
class SQLiteChangeEventSourceFactory
        implements ChangeEventSourceFactory<SQLitePartition, SQLiteOffsetContext> {

    private final SQLiteConnectorConfig config;
    private final SnapshotterService snapshotterService;
    private final MainConnectionProvidingConnectionFactory<SQLiteConnection> connectionFactory;
    private final SQLiteDatabaseSchema schema;
    private final EventDispatcher<SQLitePartition, TableId> dispatcher;
    private final Clock clock;

    SQLiteChangeEventSourceFactory(SQLiteConnectorConfig config, SnapshotterService snapshotterService,
                                   MainConnectionProvidingConnectionFactory<SQLiteConnection> connectionFactory, SQLiteDatabaseSchema schema,
                                   EventDispatcher<SQLitePartition, TableId> dispatcher, Clock clock) {
        this.config = config;
        this.snapshotterService = snapshotterService;
        this.connectionFactory = connectionFactory;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
    }

    @Override
    public SnapshotChangeEventSource<SQLitePartition, SQLiteOffsetContext> getSnapshotChangeEventSource(
                                                                                                        SnapshotProgressListener<SQLitePartition> progressListener,
                                                                                                        NotificationService<SQLitePartition, SQLiteOffsetContext> notificationService) {
        return new SQLiteSnapshotChangeEventSource(config, snapshotterService, connectionFactory, schema, dispatcher, clock, progressListener,
                notificationService);
    }

    @Override
    public StreamingChangeEventSource<SQLitePartition, SQLiteOffsetContext> getStreamingChangeEventSource() {
        return new SQLiteStreamingChangeEventSource(config);
    }
}
