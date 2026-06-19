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
 * Maps SQLite columns to Kafka Connect schemas and converts column values, switching on the
 * column's {@link SQLiteTypeAffinity}.
 *
 * <p>{@link #schemaBuilder(Column)} returns the Connect schema for a column's affinity. The value
 * converter still passes values through unchanged; per-affinity coercion is applied where rows are
 * read, not here.
 */
class SQLiteValueConverter implements ValueConverterProvider {

    /**
     * Returns the Kafka Connect {@link SchemaBuilder} for a column, chosen by its SQLite affinity:
     * INTEGER to {@code INT64}, REAL and NUMERIC to {@code FLOAT64}, TEXT to {@code STRING}, and
     * BLOB to {@code BYTES}. NUMERIC maps to {@code FLOAT64} because it matches SQLite's own numeric
     * storage and keeps the schema simple. The builder is returned without an optional flag, since
     * {@code TableSchemaBuilder} sets nullability from the column.
     *
     * @param column the column definition
     * @return the schema builder for the column's affinity
     */
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        return switch (SQLiteTypeAffinity.of(column.typeName())) {
            case INTEGER -> SchemaBuilder.int64();
            case REAL, NUMERIC -> SchemaBuilder.float64();
            case TEXT -> SchemaBuilder.string();
            case BLOB -> SchemaBuilder.bytes();
        };
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
