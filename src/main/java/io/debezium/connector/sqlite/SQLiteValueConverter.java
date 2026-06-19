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
 * <p>{@link #schemaBuilder(Column)} returns the Connect schema for a column's affinity, and
 * {@link #converter(Column, Field)} returns the matching per-affinity function that adapts a value
 * to that schema's Java type. These functions adapt a value whose storage class already agrees with
 * the column's affinity. Reconciling a value whose storage class differs from the affinity, which
 * SQLite's dynamic typing allows, is done where rows are read.
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
     * Returns the value converter for a column, chosen by its SQLite affinity. Each converter adapts
     * a value to the Java type the column's Connect schema expects: INTEGER to {@code Long}, REAL and
     * NUMERIC to {@code Double}, TEXT to {@code String}, and BLOB to {@code byte[]}. A null value is
     * passed through as null.
     *
     * @param column the column definition
     * @param field  the Connect field definition
     * @return the value converter for the column's affinity
     */
    @Override
    public ValueConverter converter(Column column, Field field) {
        return switch (SQLiteTypeAffinity.of(column.typeName())) {
            case INTEGER -> SQLiteValueConverter::toInt64;
            case REAL, NUMERIC -> SQLiteValueConverter::toFloat64;
            case TEXT -> SQLiteValueConverter::toStringValue;
            case BLOB -> SQLiteValueConverter::toBytes;
        };
    }

    /** Adapts a numeric value to the {@code Long} an INT64 schema expects, leaving null and other types untouched. */
    private static Object toInt64(Object data) {
        return data instanceof Number number ? number.longValue() : data;
    }

    /** Adapts a numeric value to the {@code Double} a FLOAT64 schema expects, leaving null and other types untouched. */
    private static Object toFloat64(Object data) {
        return data instanceof Number number ? number.doubleValue() : data;
    }

    /** Renders a value as the {@code String} a STRING schema expects, passing null through. */
    private static Object toStringValue(Object data) {
        return data == null ? null : data.toString();
    }

    /** Passes a value through for a BYTES schema; BLOB-affinity values are already {@code byte[]}. */
    private static Object toBytes(Object data) {
        return data;
    }
}
