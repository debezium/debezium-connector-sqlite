/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;

/**
 * The Kafka Connect source task for the SQLite connector.
 *
 * <p>Wires together all Debezium pipeline components and delegates snapshot and streaming
 * work to {@link SQLiteSnapshotChangeEventSource} and {@link SQLiteStreamingChangeEventSource}
 * via the {@link ChangeEventSourceCoordinator}. Full wiring is implemented in Phase 1.
 */
public class SQLiteConnectorTask extends BaseSourceTask<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteConnectorTask.class);
    private static final String CONTEXT_NAME = "sqlite-connector-task";

    private volatile SQLiteConnectorConfig connectorConfig;
    private volatile CdcSourceTaskContext<SQLiteConnectorConfig> taskContext;
    private volatile ErrorHandler errorHandler;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public CdcSourceTaskContext<SQLiteConnectorConfig> preStart(Configuration config) {
        connectorConfig = new SQLiteConnectorConfig(config);
        taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        return taskContext;
    }

    @Override
    protected ChangeEventSourceCoordinator<SQLitePartition, SQLiteOffsetContext> start(
                                                                                       Configuration config) {
        // TODO: wire up the full coordinator (queue, schema, dispatcher, coordinator) in Phase 1.
        throw new UnsupportedOperationException("SQLite connector task startup not yet implemented");
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        // TODO: poll the queue in Phase 1.
        throw new UnsupportedOperationException("SQLite connector polling not yet implemented");
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SQLiteConnectorConfig.ALL_FIELDS;
    }
}
