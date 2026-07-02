/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;

/**
 * Maps SQLite columns to Kafka Connect schemas and converts column values, switching on the
 * column's {@link SQLiteTypeAffinity}.
 *
 * <p>{@link #schemaBuilder(Column)} returns the Connect schema for a column's affinity, and
 * {@link #converter(Column, Field)} returns the matching per-affinity function that adapts a value
 * to that schema's Java type. A converter adapts a value whose storage class agrees with the
 * column's affinity. SQLite's dynamic typing lets a value's storage class differ from the affinity,
 * and a value that cannot be represented in the column's schema (a blob or non-numeric text in a
 * numeric column, a blob in a text column, or a number or text in a blob column) makes the converter
 * throw. The framework's {@code event.converting.failure.handling.mode} then decides the outcome:
 * {@code fail} stops the connector, {@code warn} logs the column and leaves the field null, and
 * {@code skip} leaves the field null quietly.
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
     * passed through as null. A value whose storage class cannot be represented in that type makes the
     * converter throw a {@link DebeziumException}, which the framework's configured event conversion
     * failure handling mode turns into a stop, a warning, or a skipped field.
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

    /** Adapts a numeric value to the {@code Long} an INT64 schema expects, passing null through and throwing for a non-numeric value. */
    private static Object toInt64(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Number number) {
            return number.longValue();
        }
        throw unrepresentable(data, "INT64");
    }

    /** Adapts a numeric value to the {@code Double} a FLOAT64 schema expects, passing null through and throwing for a non-numeric value. */
    private static Object toFloat64(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Number number) {
            return number.doubleValue();
        }
        throw unrepresentable(data, "FLOAT64");
    }

    /** Renders a value as the {@code String} a STRING schema expects, passing null through and throwing for a blob, which does not render as text. */
    private static Object toStringValue(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[]) {
            throw unrepresentable(data, "STRING");
        }
        return data.toString();
    }

    /** Passes a {@code byte[]} through for a BYTES schema, passing null through and throwing for any other type. */
    private static Object toBytes(Object data) {
        if (data == null || data instanceof byte[]) {
            return data;
        }
        throw unrepresentable(data, "BYTES");
    }

    /**
     * Builds the exception thrown when a value's storage class cannot be represented in the column's
     * schema type. SQLite's dynamic typing allows the mismatch, so the connector defers the outcome to
     * the framework's event conversion failure handling mode rather than deciding here.
     */
    private static DebeziumException unrepresentable(Object data, String schemaType) {
        return new DebeziumException("A " + data.getClass().getSimpleName() + " value does not match the "
                + "column's " + schemaType + " schema; its SQLite storage class differs from the column's affinity");
    }
}
