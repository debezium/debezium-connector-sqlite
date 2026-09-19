/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.List;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Maintains the Kafka Connect schemas (key, value, envelope) for each table the connector tracks. The
 * {@link io.debezium.pipeline.EventDispatcher} calls {@link #schemaFor(TableId)} before dispatching
 * each event.
 */
public class SQLiteDatabaseSchema extends RelationalDatabaseSchema {

    public SQLiteDatabaseSchema(CdcSourceTaskContext<SQLiteConnectorConfig> taskContext,
                                TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(taskContext.getConfig(),
                topicNamingStrategy,
                taskContext.getConfig().getTableFilters().dataCollectionFilter(),
                taskContext.getConfig().getColumnFilter(),
                new TableSchemaBuilder(
                        new SQLiteValueConverter(taskContext.getConfig().shouldSubstituteNonNullPlaceholder()),
                        null,
                        taskContext.getConfig().schemaNameAdjuster(),
                        new CustomConverterRegistry(List.of()),
                        taskContext.getConfig().getSourceInfoStructMaker().schema(),
                        taskContext.getConfig().getFieldNamer(),
                        false,
                        taskContext.getConfig().getEventConvertingFailureHandlingMode()),
                false,
                taskContext.getConfig().getKeyMapper(),
                taskContext);
    }

    /**
     * Reads the current schema from the database and registers a Kafka Connect schema for every
     * monitored table.
     *
     * @param connection an open connection to the database file
     * @throws SQLException if the schema cannot be read
     */
    public void refresh(SQLiteConnection connection) throws SQLException {
        connection.readSchema(tables(), null, null, getTableFilter(), null, true);
        tableIds().forEach(this::refreshSchema);
    }

    /**
     * Reads the tables that currently exist in the database, filtered, into a fresh {@link Tables} without
     * changing the shapes this schema emits against. The streaming loop diffs this against the emitted
     * shapes to decide which shape swaps to defer until streaming drains past the change_id where the
     * schema changed.
     *
     * @param connection an open connection to the database file
     * @return the tables that currently exist in the database
     * @throws SQLException if the schema cannot be read
     */
    public Tables readDatabaseTables(SQLiteConnection connection) throws SQLException {
        Tables current = new Tables();
        connection.readSchema(current, null, null, getTableFilter(), null, true);
        return current;
    }

    /** Registers {@code table} as the shape emitted for its id, replacing any shape already registered. */
    public void registerTable(Table table) {
        tables().overwriteTable(table);
        buildAndRegisterSchema(table);
    }

    /** Removes {@code id} from the emitted schema, so it is no longer tracked or emitted. */
    public void evictTable(TableId id) {
        removeSchema(id);
        tables().removeTable(id);
    }
}
