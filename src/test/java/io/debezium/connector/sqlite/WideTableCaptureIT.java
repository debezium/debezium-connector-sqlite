/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;

/**
 * Proves a wide table (70 columns, past one {@code json_object} call) is captured across chunks with
 * every column kept: nulls in the first and a later chunk, a blob in a later chunk, and a column name
 * carrying a double quote.
 */
public class WideTableCaptureIT {

    private static final int COLUMN_COUNT = 70;
    private static final int BLOB_INDEX = 60;
    private static final int AWKWARD_INDEX = 55;
    private static final String AWKWARD_NAME = "zzq\"col";
    private static final int NULL_IN_FIRST_CHUNK = 10;
    private static final int NULL_IN_LATER_CHUNK = 58;

    @Test
    void wideRowKeepsEveryColumnAcrossChunks() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute(createWideTable());
            helper.installTriggers("wide");

            db.execute(insertWideRow());

            String newRow = readNewRowData(db);

            // A null survives the json_set merge in both the first and a later chunk.
            assertThat(newRow).contains("\"c" + NULL_IN_FIRST_CHUNK + "\":null");
            assertThat(newRow).contains("\"c" + NULL_IN_LATER_CHUNK + "\":null");
            assertThat(newRow).contains("\"c" + BLOB_INDEX + "\":{\"" + CdcLog.BLOB_HEX_MARKER + "\":\"AB\"}");
            assertThat(newRow).contains("\"zzq\\\"col\":\"ok\"");
            assertThat(newRow).contains("\"c" + (COLUMN_COUNT - 1) + "\":\"v" + (COLUMN_COUNT - 1) + "\"");
        }
    }

    private static String columnName(int index) {
        return index == AWKWARD_INDEX ? AWKWARD_NAME : "c" + index;
    }

    private static String quoted(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String createWideTable() {
        List<String> defs = new ArrayList<>();
        for (int i = 0; i < COLUMN_COUNT; i++) {
            String type = i == 0 ? "INTEGER PRIMARY KEY" : i == BLOB_INDEX ? "BLOB" : "TEXT";
            defs.add(quoted(columnName(i)) + " " + type);
        }
        return "CREATE TABLE wide (" + String.join(", ", defs) + ")";
    }

    private static String insertWideRow() {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < COLUMN_COUNT; i++) {
            names.add(quoted(columnName(i)));
            values.add(valueFor(i));
        }
        return "INSERT INTO wide (" + String.join(", ", names) + ") VALUES ("
                + String.join(", ", values) + ")";
    }

    private static String valueFor(int index) {
        if (index == 0) {
            return "1";
        }
        if (index == BLOB_INDEX) {
            return "x'AB'";
        }
        if (index == NULL_IN_FIRST_CHUNK || index == NULL_IN_LATER_CHUNK) {
            return "NULL";
        }
        if (index == AWKWARD_INDEX) {
            return "'ok'";
        }
        return "'v" + index + "'";
    }

    private static String readNewRowData(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.NEW_ROW_DATA + " FROM " + CdcLog.TABLE_NAME;
        return db.queryAndMap(query, rs -> {
            rs.next();
            return rs.getString(1);
        });
    }
}
