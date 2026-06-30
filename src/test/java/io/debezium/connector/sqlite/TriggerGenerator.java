/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds the SQLite triggers that stand in for a change-event writer in tests.
 *
 * <p>Given a source table and its columns, {@link #createTriggers(String, List)} returns the
 * {@code AFTER INSERT}, {@code AFTER UPDATE}, and {@code AFTER DELETE} trigger statements that write
 * one correctly shaped row into {@link CdcLog#TABLE_NAME _debezium_cdc_log} for every change to the
 * table. The row data is captured with SQLite's {@code json_object()} over the {@code NEW} and
 * {@code OLD} row aliases, so an insert records the new row, a delete records the old row, and an
 * update records both.
 *
 * <p>This class only builds the SQL. Running it against a database is the caller's job.
 */
public final class TriggerGenerator {

    /** Prefix for generated trigger names, kept distinct so the triggers are easy to recognize. */
    private static final String TRIGGER_PREFIX = "_debezium_cdc_";

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
        String newRow = jsonObject("NEW", columns);
        String oldRow = jsonObject("OLD", columns);
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

    /** Builds a {@code json_object('col', ALIAS."col", ...)} expression over the row alias. */
    private static String jsonObject(String rowAlias, List<String> columns) {
        String pairs = columns.stream()
                .map(column -> "'" + column + "', " + rowAlias + ".\"" + column + "\"")
                .collect(Collectors.joining(", "));
        return "json_object(" + pairs + ")";
    }
}
