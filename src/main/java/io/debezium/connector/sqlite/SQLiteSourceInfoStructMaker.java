/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Builds the Kafka Connect schema and struct for the {@code source} field of every change event.
 * Fields added here must also be stored in {@link SQLiteSourceInfo}.
 */
class SQLiteSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SQLiteSourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig config) {
        super.init(connector, version, config);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.sqlite.Source")
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SQLiteSourceInfo.CHANGE_ID_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SQLiteSourceInfo info) {
        return commonStruct(info)
                .put(AbstractSourceInfo.TABLE_NAME_KEY, info.tableName() != null ? info.tableName() : "")
                .put(SQLiteSourceInfo.CHANGE_ID_KEY, info.changeId());
    }
}
