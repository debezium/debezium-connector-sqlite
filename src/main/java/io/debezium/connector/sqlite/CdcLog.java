/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

/**
 * The contract for the {@code _debezium_cdc_log} table.
 *
 * <p>This append-only table holds the change events the connector reads over plain JDBC. Each row
 * records one change: the source table, the operation, the row data before and after, and when it
 * was committed. The table name, column names, and DDL are defined here in one place so the writer
 * of the table and the reader of it agree on a single shape. This class is constants only and is
 * never instantiated.
 */
public final class CdcLog {

    /** Name of the CDC log table. */
    public static final String TABLE_NAME = "_debezium_cdc_log";

    /** Auto-incrementing primary key, the connector's log sequence number and offset value. */
    public static final String CHANGE_ID = "change_id";

    /** Source table the change came from. */
    public static final String TABLE_NAME_COLUMN = "table_name";

    /** Operation code: {@code c} (create), {@code u} (update), or {@code d} (delete). */
    public static final String OPERATION = "operation";

    /** JSON of the row before the change; {@code NULL} for inserts. */
    public static final String OLD_ROW_DATA = "old_row_data";

    /** JSON of the row after the change; {@code NULL} for deletes. */
    public static final String NEW_ROW_DATA = "new_row_data";

    /** Commit time of the change in Unix epoch milliseconds. */
    public static final String COMMITTED_AT = "committed_at";

    /** {@link #OPERATION} value for an insert. */
    public static final String OPERATION_CREATE = "c";

    /** {@link #OPERATION} value for an update. */
    public static final String OPERATION_UPDATE = "u";

    /** {@link #OPERATION} value for a delete. */
    public static final String OPERATION_DELETE = "d";

    /**
     * The frozen {@code CREATE TABLE} statement. Built from the column constants above so the DDL
     * and the names used to read the table can never drift apart. {@code IF NOT EXISTS} makes it
     * safe to run on every startup.
     */
    public static final String CREATE_TABLE_DDL = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                %s INTEGER PRIMARY KEY AUTOINCREMENT,
                %s TEXT NOT NULL,
                %s TEXT NOT NULL,
                %s TEXT,
                %s TEXT,
                %s INTEGER NOT NULL
            )""",
            TABLE_NAME, CHANGE_ID, TABLE_NAME_COLUMN, OPERATION, OLD_ROW_DATA, NEW_ROW_DATA, COMMITTED_AT);

    private CdcLog() {
    }
}
