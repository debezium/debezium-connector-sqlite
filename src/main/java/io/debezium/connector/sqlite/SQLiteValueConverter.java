/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;

/**
 * Maps SQLite column types to Kafka Connect schemas and converts column values.
 *
 * <p>Stub implementation for Phase 0. Full type mapping (INTEGER, REAL, TEXT, BLOB, NUMERIC)
 * is implemented in Phase 1.
 */
class SQLiteValueConverter implements ValueConverterProvider {

    /**
     * Returns a {@link SchemaBuilder} for the given SQLite column type.
     *
     * @param column the column definition
     * @return the schema builder, or {@code null} if the type is not yet mapped
     */
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        // TODO: map SQLite affinities (INTEGER, REAL, TEXT, BLOB, NUMERIC) in Phase 1.
        return null;
    }

    /**
     * Returns a converter that transforms a raw SQLite value into the Connect-typed value
     * described by the schema returned from {@link #schemaBuilder(Column)}.
     *
     * @param column the column definition
     * @param field  the Connect field definition
     * @return the value converter
     */
    @Override
    public ValueConverter converter(Column column, Field field) {
        // TODO: implement type-specific converters in Phase 1.
        return x -> x;
    }
}
