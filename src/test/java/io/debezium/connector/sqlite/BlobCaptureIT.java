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
 * Proves the trigger captures a blob losslessly. A plain {@code json_object} cannot hold raw bytes and
 * would fail the user's write, so the trigger hex-encodes a blob into the tagged
 * {@code {"__dbz_hex__": "<hex>"}} object. This test writes a row with a blob and a row with a null blob,
 * then reads the JSON the trigger wrote and checks its shape directly, with no streaming or decode code.
 * It uses a real temp database, so it runs under Failsafe.
 */
public class BlobCaptureIT {

    @Test
    void blobIsCapturedAsTaggedHexAndNullBlobStaysJsonNull() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, data BLOB)");
            helper.installTriggers("files");

            // A blob and a null blob. The write must succeed rather than roll back on the blob.
            db.execute("INSERT INTO files (id, data) VALUES (1, x'DEADBEEF')");
            db.execute("INSERT INTO files (id, data) VALUES (2, NULL)");

            List<String> newRows = readNewRowData(db);
            assertThat(newRows).hasSize(2);

            // The blob is hex-encoded, uppercase, inside the tagged object, byte for byte.
            assertThat(newRows.get(0)).contains("\"data\":{\"" + CdcLog.BLOB_HEX_MARKER + "\":\"DEADBEEF\"}");
            // A null blob stays a bare JSON null, not the tagged object.
            assertThat(newRows.get(1)).contains("\"data\":null");
        }
    }

    /** Reads every {@code new_row_data} JSON value, ordered by {@code change_id}. */
    private static List<String> readNewRowData(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.NEW_ROW_DATA + " FROM " + CdcLog.TABLE_NAME
                + " ORDER BY " + CdcLog.CHANGE_ID;
        return db.queryAndMap(query, rs -> {
            List<String> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(rs.getString(1));
            }
            return rows;
        });
    }
}
