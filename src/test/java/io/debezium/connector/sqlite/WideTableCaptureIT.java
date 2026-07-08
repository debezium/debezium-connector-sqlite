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
 * Proves a wide table is captured correctly. With more columns than one {@code json_object} call can
 * hold, the trigger serializes the row in chunks and merges them with {@code json_set}. This test
 * builds a 70 column table, so more than one chunk is needed, and checks that every column survives:
 * nulls in both the first and a later chunk are preserved, a blob in a later chunk stays tagged, and a
 * column whose name carries a double quote is addressed correctly. It reads the JSON the trigger writes
 * directly, with no streaming or decode code. It uses a real temp database, so it runs under Failsafe.
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

            // A null is kept in the first chunk (json_object) and in a later chunk (json_set), rather
            // than being dropped by the merge.
            assertThat(newRow).contains("\"c" + NULL_IN_FIRST_CHUNK + "\":null");
            assertThat(newRow).contains("\"c" + NULL_IN_LATER_CHUNK + "\":null");
            // A blob in a later chunk stays the tagged hex object through the json_set merge.
            assertThat(newRow).contains("\"c" + BLOB_INDEX + "\":{\"" + CdcLog.BLOB_HEX_MARKER + "\":\"AB\"}");
            // A column name carrying a double quote is addressed correctly by the json_set path.
            assertThat(newRow).contains("\"zzq\\\"col\":\"ok\"");
            // The last column is present, so nothing was lost off the end of the row.
            assertThat(newRow).contains("\"c" + (COLUMN_COUNT - 1) + "\":\"v" + (COLUMN_COUNT - 1) + "\"");
        }
    }

    private static String columnName(int index) {
        return index == AWKWARD_INDEX ? AWKWARD_NAME : "c" + index;
    }

    /** A quoted SQL identifier, with any embedded double quote doubled. */
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

    /** Reads the single {@code new_row_data} JSON value. */
    private static String readNewRowData(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.NEW_ROW_DATA + " FROM " + CdcLog.TABLE_NAME;
        return db.queryAndMap(query, rs -> {
            rs.next();
            return rs.getString(1);
        });
    }
}
