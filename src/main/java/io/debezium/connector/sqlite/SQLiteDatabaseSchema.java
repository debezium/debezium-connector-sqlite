/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Maintains the Kafka Connect schemas (key, value, envelope) for each table tracked by the
 * SQLite connector.
 *
 * <p>Call {@link #buildAndRegisterSchema(io.debezium.relational.Table)} whenever a table's
 * structure becomes known (typically during snapshot or after a schema change). The
 * {@link io.debezium.pipeline.EventDispatcher} calls {@link #schemaFor(TableId)} to look up the
 * schema before dispatching each event.
 */
public class SQLiteDatabaseSchema extends RelationalDatabaseSchema {

    public SQLiteDatabaseSchema(CdcSourceTaskContext<SQLiteConnectorConfig> taskContext,
                                TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(taskContext.getConfig(),
                topicNamingStrategy,
                taskContext.getConfig().getTableFilters().dataCollectionFilter(),
                taskContext.getConfig().getColumnFilter(),
                new TableSchemaBuilder(
                        new SQLiteValueConverter(),
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
}
