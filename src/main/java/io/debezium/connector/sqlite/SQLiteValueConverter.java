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
 * <p>SQLite's dynamic typing lets a value's storage class differ from the column's affinity. A value
 * that cannot be represented in the column's schema makes the converter throw, which the framework's
 * {@code event.converting.failure.handling.mode} turns into a stop, a warning, or a skipped field.
 * When {@code nonnull.affinity.mismatch.fallback} is enabled, a non-nullable column with no default
 * gets a type placeholder instead so the value keeps flowing.
 */
class SQLiteValueConverter implements ValueConverterProvider {

    private static final byte[] EMPTY_BYTES = new byte[0];

    /** Whether to substitute a type placeholder for an unrepresentable value in a non-nullable, no-default column. */
    private final boolean substituteNonNullFallback;

    SQLiteValueConverter(boolean substituteNonNullFallback) {
        this.substituteNonNullFallback = substituteNonNullFallback;
    }

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
     * converter throw a {@link DebeziumException}, unless the column is non-nullable with no default
     * and placeholder substitution is enabled, in which case a type placeholder is substituted. The
     * throw is turned into a stop, a warning, or a skipped field by the framework's configured event
     * conversion failure handling mode.
     *
     * @param column the column definition
     * @param field  the Connect field definition
     * @return the value converter for the column's affinity
     */
    @Override
    public ValueConverter converter(Column column, Field field) {
        SQLiteTypeAffinity affinity = SQLiteTypeAffinity.of(column.typeName());
        return data -> convert(affinity, column, field, data);
    }

    /**
     * Adapts a value to the Java type the column's schema expects, passing null through. A value whose
     * storage class does not fit the affinity yields a placeholder when the column is non-nullable with
     * no default and the fallback is enabled, and otherwise throws.
     */
    private Object convert(SQLiteTypeAffinity affinity, Column column, Field field, Object data) {
        if (data == null) {
            return null;
        }
        Object converted = tryConvert(affinity, data);
        if (converted != null) {
            return converted;
        }
        if (substituteNonNullFallback && isRequiredWithoutDefault(column, field)) {
            return fallbackFor(affinity);
        }
        throw unrepresentable(data, affinity);
    }

    /**
     * Converts a non-null value to the affinity's Java type, or returns null when the value's storage
     * class cannot be represented in that type. A successful conversion is never null, so a null return
     * unambiguously marks an unrepresentable value.
     */
    private static Object tryConvert(SQLiteTypeAffinity affinity, Object data) {
        return switch (affinity) {
            case INTEGER -> data instanceof Number number ? number.longValue() : null;
            case REAL, NUMERIC -> data instanceof Number number ? number.doubleValue() : null;
            case TEXT -> data instanceof byte[] ? null : data.toString();
            case BLOB -> data instanceof byte[] ? data : null;
        };
    }

    /** The type placeholder for an affinity: 0 for INTEGER, 0.0 for REAL and NUMERIC, an empty string for TEXT, and empty bytes for BLOB. */
    private static Object fallbackFor(SQLiteTypeAffinity affinity) {
        return switch (affinity) {
            case INTEGER -> 0L;
            case REAL, NUMERIC -> 0.0d;
            case TEXT -> "";
            case BLOB -> EMPTY_BYTES;
        };
    }

    /** A column that must hold a value has no null to fall back on when it is non-nullable and carries no default. */
    private static boolean isRequiredWithoutDefault(Column column, Field field) {
        return !column.isOptional() && (field == null || field.schema().defaultValue() == null);
    }

    /**
     * Builds the exception thrown when a value's storage class cannot be represented in the column's
     * schema type. SQLite's dynamic typing allows the mismatch, so the connector defers the outcome to
     * the framework's event conversion failure handling mode rather than deciding here.
     */
    private static DebeziumException unrepresentable(Object data, SQLiteTypeAffinity affinity) {
        return new DebeziumException("A " + data.getClass().getSimpleName() + " value does not match the "
                + "column's " + schemaTypeName(affinity) + " schema; its SQLite storage class differs from the column's affinity");
    }

    /** The Connect schema type name for an affinity, used in the mismatch message. */
    private static String schemaTypeName(SQLiteTypeAffinity affinity) {
        return switch (affinity) {
            case INTEGER -> "INT64";
            case REAL, NUMERIC -> "FLOAT64";
            case TEXT -> "STRING";
            case BLOB -> "BYTES";
        };
    }
}
