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

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        return switch (SQLiteTypeAffinity.of(column.typeName())) {
            case INTEGER -> SchemaBuilder.int64();
            case REAL, NUMERIC -> SchemaBuilder.float64();
            case TEXT -> SchemaBuilder.string();
            case BLOB -> SchemaBuilder.bytes();
        };
    }

    @Override
    public ValueConverter converter(Column column, Field field) {
        SQLiteTypeAffinity affinity = SQLiteTypeAffinity.of(column.typeName());
        return data -> convert(affinity, column, field, data);
    }

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

    /** Converts a non-null value to the affinity's Java type, or returns null if it cannot be represented. */
    private static Object tryConvert(SQLiteTypeAffinity affinity, Object data) {
        return switch (affinity) {
            case INTEGER -> data instanceof Number number ? number.longValue() : null;
            case REAL, NUMERIC -> data instanceof Number number ? number.doubleValue() : null;
            case TEXT -> data instanceof byte[] ? null : data.toString();
            case BLOB -> data instanceof byte[] ? data : null;
        };
    }

    private static Object fallbackFor(SQLiteTypeAffinity affinity) {
        return switch (affinity) {
            case INTEGER -> 0L;
            case REAL, NUMERIC -> 0.0d;
            case TEXT -> "";
            case BLOB -> EMPTY_BYTES;
        };
    }

    private static boolean isRequiredWithoutDefault(Column column, Field field) {
        return !column.isOptional() && (field == null || field.schema().defaultValue() == null);
    }

    private static DebeziumException unrepresentable(Object data, SQLiteTypeAffinity affinity) {
        return new DebeziumException("A " + data.getClass().getSimpleName() + " value does not match the "
                + "column's " + schemaTypeName(affinity) + " schema; its SQLite storage class differs from the column's affinity");
    }

    private static String schemaTypeName(SQLiteTypeAffinity affinity) {
        return switch (affinity) {
            case INTEGER -> "INT64";
            case REAL, NUMERIC -> "FLOAT64";
            case TEXT -> "STRING";
            case BLOB -> "BYTES";
        };
    }
}
