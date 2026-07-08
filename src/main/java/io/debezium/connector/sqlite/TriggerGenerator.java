/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds the SQLite triggers that capture changes into {@link CdcLog#TABLE_NAME _debezium_cdc_log}.
 *
 * <p>Given a source table and its columns, {@link #createTriggers(String, List)} returns the
 * {@code AFTER INSERT}, {@code AFTER UPDATE}, and {@code AFTER DELETE} trigger statements that write
 * one correctly shaped row into {@code _debezium_cdc_log} for every change to the table. The row data
 * is captured with SQLite's {@code json_object()} over the {@code NEW} and {@code OLD} row aliases, so
 * an insert records the new row, a delete records the old row, and an update records both.
 *
 * <p>This class only builds the SQL. Installing the triggers against a database is the caller's job.
 */
public final class TriggerGenerator {

    /** Prefix for generated trigger names, kept distinct so the triggers are easy to recognize. */
    private static final String TRIGGER_PREFIX = "_debezium_cdc_";

    /**
     * Columns per {@code json_object} call for a wide table. {@code json_object} accepts at most 127
     * arguments, which is 63 columns (two arguments each), so a row past that is serialized in chunks
     * and merged. This is set conservatively below 63 to leave headroom.
     */
    private static final int MAX_COLUMNS_PER_CHUNK = 50;

    /** The {@code _debezium_cdc_log} columns the triggers write, in insert order. */
    private static final String TARGET_COLUMNS = String.join(", ",
            CdcLog.TABLE_NAME_COLUMN, CdcLog.OPERATION, CdcLog.OLD_ROW_DATA, CdcLog.NEW_ROW_DATA, CdcLog.COMMITTED_AT);

    /**
     * Commit time as Unix epoch milliseconds. {@code unixepoch('now', 'subsec')} gives epoch seconds
     * with a fractional millisecond part, so scaling by 1000 yields epoch milliseconds.
     */
    private static final String COMMITTED_AT_EXPR = "CAST(unixepoch('now', 'subsec') * 1000 AS INTEGER)";

    private TriggerGenerator() {
    }

    /**
     * Builds the insert, update, and delete triggers for one source table.
     *
     * @param tableName the source table the triggers watch
     * @param columns the source table's column names, captured into the JSON row data
     * @return the three {@code CREATE TRIGGER} statements, in insert, update, delete order
     */
    public static List<String> createTriggers(String tableName, List<String> columns) {
        String newRow = rowJson("NEW", columns);
        String oldRow = rowJson("OLD", columns);
        return List.of(
                trigger(tableName, "insert", "INSERT", CdcLog.OPERATION_CREATE, "NULL", newRow),
                trigger(tableName, "update", "UPDATE", CdcLog.OPERATION_UPDATE, oldRow, newRow),
                trigger(tableName, "delete", "DELETE", CdcLog.OPERATION_DELETE, oldRow, "NULL"));
    }

    /** Builds one {@code CREATE TRIGGER} statement for the given operation. */
    private static String trigger(String tableName, String suffix, String timing,
                                  String operation, String oldData, String newData) {
        return String.format("""
                CREATE TRIGGER IF NOT EXISTS %s
                AFTER %s ON "%s"
                BEGIN
                    INSERT INTO %s (%s)
                    VALUES ('%s', '%s', %s, %s, %s);
                END""",
                TRIGGER_PREFIX + tableName + "_" + suffix,
                timing,
                tableName,
                CdcLog.TABLE_NAME,
                TARGET_COLUMNS,
                tableName,
                operation,
                oldData,
                newData,
                COMMITTED_AT_EXPR);
    }

    /**
     * The row's JSON over the given alias. A narrow row is one {@code json_object} call. A row with
     * more columns than one call can hold is serialized in chunks and merged with {@code json_set},
     * which keeps null columns rather than dropping them the way {@code json_patch} would.
     */
    private static String rowJson(String rowAlias, List<String> columns) {
        List<List<String>> chunks = partition(columns, MAX_COLUMNS_PER_CHUNK);
        String json = jsonObject(rowAlias, chunks.get(0));
        for (int i = 1; i < chunks.size(); i++) {
            json = jsonSet(json, rowAlias, chunks.get(i));
        }
        return json;
    }

    /** Builds a {@code json_object('col', <value>, ...)} call for one chunk of columns. */
    private static String jsonObject(String rowAlias, List<String> columns) {
        String pairs = columns.stream()
                .map(column -> sqlString(column) + ", " + columnValue(rowAlias, column))
                .collect(Collectors.joining(", "));
        return "json_object(" + pairs + ")";
    }

    /**
     * Merges one chunk of columns onto an existing JSON expression with {@code json_set}. A null value
     * is set as JSON null and kept, so no column is lost from a wide row. The nested {@code json_object}
     * a blob column produces embeds as real JSON, not as a string, so a blob captured through this path
     * stays the tagged object.
     */
    private static String jsonSet(String json, String rowAlias, List<String> columns) {
        String assignments = columns.stream()
                .map(column -> jsonPath(column) + ", " + columnValue(rowAlias, column))
                .collect(Collectors.joining(", "));
        return "json_set(" + json + ", " + assignments + ")";
    }

    /**
     * The captured value expression for one column. A blob is hex-encoded into a tagged nested object
     * so {@code json_object} can hold it, which it otherwise cannot; every other storage class stays a
     * bare value. The {@code typeof} test runs per row, so a blob is caught in any column whatever its
     * declared affinity.
     */
    private static String columnValue(String rowAlias, String column) {
        String ref = columnRef(rowAlias, column);
        return "CASE WHEN typeof(" + ref + ")='blob' THEN json_object('" + CdcLog.BLOB_HEX_MARKER
                + "', hex(" + ref + ")) ELSE " + ref + " END";
    }

    /** A quoted identifier reference {@code ALIAS."col"}, with any double quote in the name doubled. */
    private static String columnRef(String rowAlias, String column) {
        return rowAlias + ".\"" + column.replace("\"", "\"\"") + "\"";
    }

    /** A SQL string literal for the value, with any single quote doubled. */
    private static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * A {@code json_set} path literal {@code '$."col"'} that addresses one column by name. The name is
     * escaped for the JSON path (backslash and double quote), then the whole path is escaped for the
     * SQL string literal (single quote), so an awkward column name still addresses the right key.
     */
    private static String jsonPath(String column) {
        String key = column.replace("\\", "\\\\").replace("\"", "\\\"");
        return sqlString("$.\"" + key + "\"");
    }

    /** Splits the columns into groups of at most {@code size}, preserving order. */
    private static List<List<String>> partition(List<String> columns, int size) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < columns.size(); i += size) {
            chunks.add(columns.subList(i, Math.min(i + size, columns.size())));
        }
        return chunks;
    }
}
