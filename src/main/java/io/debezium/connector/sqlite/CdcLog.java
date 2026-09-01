/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

/**
 * The contract for the {@code _debezium_cdc_log} table: an append-only table whose rows each record
 * one change. The table name, column names, and DDL are defined here so the writer and reader agree
 * on one shape. Constants only; never instantiated.
 */
public final class CdcLog {

    public static final String TABLE_NAME = "_debezium_cdc_log";

    /** Auto-incrementing primary key; the connector's offset value. */
    public static final String CHANGE_ID = "change_id";

    public static final String TABLE_NAME_COLUMN = "table_name";

    /** Operation code: {@code c} (create), {@code u} (update), or {@code d} (delete). */
    public static final String OPERATION = "operation";

    /** JSON of the row before the change; {@code NULL} for inserts. */
    public static final String OLD_ROW_DATA = "old_row_data";

    /** JSON of the row after the change; {@code NULL} for deletes. */
    public static final String NEW_ROW_DATA = "new_row_data";

    /** Commit time of the change in Unix epoch milliseconds. */
    public static final String COMMITTED_AT = "committed_at";

    public static final String OPERATION_CREATE = "c";

    public static final String OPERATION_UPDATE = "u";

    public static final String OPERATION_DELETE = "d";

    /**
     * Marker key for a blob value in the row JSON. {@code json_object} cannot hold raw bytes, so a
     * blob is captured hex-encoded inside a tagged nested object, {@code {"__dbz_hex__": "<hex>"}}.
     * A non-blob value is a bare JSON scalar, so the two can never be confused on read-back.
     */
    public static final String BLOB_HEX_MARKER = "__dbz_hex__";

    /**
     * The frozen {@code CREATE TABLE} statement, built from the column constants so the DDL and the
     * read names cannot drift. {@code IF NOT EXISTS} makes it safe to run on every startup.
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
