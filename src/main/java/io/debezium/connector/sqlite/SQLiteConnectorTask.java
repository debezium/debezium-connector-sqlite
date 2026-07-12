/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProviderService;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The Kafka Connect source task for the SQLite connector.
 *
 * <p>Wires together all Debezium pipeline components and delegates snapshot and streaming
 * work to {@link SQLiteSnapshotChangeEventSource} and {@link SQLiteStreamingChangeEventSource}
 * via the {@link ChangeEventSourceCoordinator}.
 */
public class SQLiteConnectorTask extends BaseSourceTask<SQLitePartition, SQLiteOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteConnectorTask.class);
    private static final String CONTEXT_NAME = "sqlite-connector-task";

    private volatile SQLiteConnectorConfig connectorConfig;
    private volatile CdcSourceTaskContext<SQLiteConnectorConfig> taskContext;
    private volatile SQLiteConnection connection;
    private volatile SQLiteDatabaseSchema schema;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
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
    protected ChangeEventSourceCoordinator<SQLitePartition, SQLiteOffsetContext> start(Configuration config) {
        final String databaseFilePath = config.getString(SQLiteConnectorConfig.DATABASE_FILE);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        // Service providers must be registered before any service is looked up below.
        registerServiceProviders(connectorConfig.getServiceRegistry());

        // Open the database and run the startup prerequisites before wiring the pipeline. The factory
        // provides the main connection the snapshot source reads from, so the snapshot shares this
        // already-prepared connection.
        final MainConnectionProvidingConnectionFactory<SQLiteConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new SQLiteConnection(databaseFilePath));
        connection = connectionFactory.mainConnection();
        try {
            connection.connect();
            connection.enforceWalMode();
            connection.createCdcLogTable();
            connection.verifyMinimumVersion();
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to initialize the SQLite database at " + databaseFilePath, e);
        }

        this.schema = new SQLiteDatabaseSchema(taskContext, topicNamingStrategy);
        try {
            this.schema.refresh(connection);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to load the SQLite schema from " + databaseFilePath, e);
        }

        final Offsets<SQLitePartition, SQLiteOffsetContext> previousOffsets = getPreviousOffsets(
                new SQLitePartition.Provider(connectorConfig),
                new SQLiteOffsetLoader(connectorConfig));

        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, connection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .queueProvider(connectorConfig.getServiceRegistry().tryGetService(QueueProviderService.class).getQueueProvider())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        this.errorHandler = new SQLiteErrorHandler(connectorConfig, queue, errorHandler);

        final SQLiteEventMetadataProvider metadataProvider = new SQLiteEventMetadataProvider();

        final SignalProcessor<SQLitePartition, SQLiteOffsetContext> signalProcessor = new SignalProcessor<>(
                SQLiteSourceConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final EventDispatcher<SQLitePartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final NotificationService<SQLitePartition, SQLiteOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        final Clock clock = Clock.system();

        final ChangeEventSourceCoordinator<SQLitePartition, SQLiteOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                SQLiteSourceConnector.class,
                connectorConfig,
                new SQLiteChangeEventSourceFactory(connectorConfig, snapshotterService, connectionFactory, schema, dispatcher, clock),
                new DefaultChangeEventSourceMetricsFactory<>(),
                dispatcher,
                schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        return records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing the SQLite connection", e);
        }

        if (schema != null) {
            schema.close();
        }

        if (queue != null) {
            queue.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SQLiteConnectorConfig.ALL_FIELDS;
    }
}
