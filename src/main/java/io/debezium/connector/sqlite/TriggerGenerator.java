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
 * {@link #createTriggers(String, List)} returns the {@code AFTER INSERT}, {@code AFTER UPDATE}, and
 * {@code AFTER DELETE} statements that write one row per change, using {@code json_object()} over the
 * {@code NEW} and {@code OLD} aliases. This class only builds the SQL; installing it is the caller's job.
 */
public final class TriggerGenerator {

    /** Prefix for generated trigger names, kept distinct so the triggers are easy to recognize. */
    static final String TRIGGER_PREFIX = "_debezium_cdc_";

    /**
     * Columns per {@code json_object} call. It accepts at most 127 arguments, so 63 columns; a wider
     * row is serialized in chunks and merged. Set below 63 to leave headroom.
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

    /** Builds the insert, update, and delete triggers for one source table, in that order. */
    public static List<String> createTriggers(String tableName, List<String> columns) {
        String newRow = rowJson("NEW", columns);
        String oldRow = rowJson("OLD", columns);
        return List.of(
                trigger(tableName, "insert", "INSERT", CdcLog.OPERATION_CREATE, "NULL", newRow),
                trigger(tableName, "update", "UPDATE", CdcLog.OPERATION_UPDATE, oldRow, newRow),
                trigger(tableName, "delete", "DELETE", CdcLog.OPERATION_DELETE, oldRow, "NULL"));
    }

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
     * The row's JSON over the given alias. A row wider than one {@code json_object} call is merged with
     * {@code json_set}, which keeps null columns rather than dropping them the way {@code json_patch} would.
     */
    private static String rowJson(String rowAlias, List<String> columns) {
        List<List<String>> chunks = partition(columns, MAX_COLUMNS_PER_CHUNK);
        String json = jsonObject(rowAlias, chunks.get(0));
        for (int i = 1; i < chunks.size(); i++) {
            json = jsonSet(json, rowAlias, chunks.get(i));
        }
        return json;
    }

    private static String jsonObject(String rowAlias, List<String> columns) {
        String pairs = columns.stream()
                .map(column -> sqlString(column) + ", " + columnValue(rowAlias, column))
                .collect(Collectors.joining(", "));
        return "json_object(" + pairs + ")";
    }

    /**
     * Merges a chunk onto the JSON with {@code json_set}, which keeps null columns rather than dropping
     * them. A blob column's nested {@code json_object} embeds as real JSON, so it stays the tagged object.
     */
    private static String jsonSet(String json, String rowAlias, List<String> columns) {
        String assignments = columns.stream()
                .map(column -> jsonPath(column) + ", " + columnValue(rowAlias, column))
                .collect(Collectors.joining(", "));
        return "json_set(" + json + ", " + assignments + ")";
    }

    /**
     * The captured value for one column. A blob is hex-encoded into a tagged nested object, which
     * {@code json_object} can otherwise not hold; every other storage class stays a bare value. The
     * {@code typeof} test runs per row, so a blob is caught whatever the column's declared affinity.
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
     * A {@code json_set} path literal {@code '$."col"'} for one column. The name is escaped for the JSON
     * path (backslash and double quote), then the path is escaped for the SQL string literal (single quote).
     */
    private static String jsonPath(String column) {
        String key = column.replace("\\", "\\\\").replace("\"", "\\\"");
        return sqlString("$.\"" + key + "\"");
    }

    private static List<List<String>> partition(List<String> columns, int size) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < columns.size(); i += size) {
            chunks.add(columns.subList(i, Math.min(i + size, columns.size())));
        }
        return chunks;
    }
}
