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
 * Proves the development and test loop end to end: with the triggers the helper installs, a plain SQL
 * insert, update, and delete on a source table each land the matching {@code c}, {@code u}, and
 * {@code d} row in {@code _debezium_cdc_log}, with the right table name and JSON row data. This is the
 * Phase 0 exit gate. It uses a real temp database, so it runs under Failsafe.
 */
public class CdcCaptureIT {

    @Test
    void insertUpdateDeleteAreCapturedIntoCdcLog() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
            helper.installTriggers("customers");

            db.execute("INSERT INTO customers (id, name) VALUES (1, 'Alice')");
            db.execute("UPDATE customers SET name = 'Alicia' WHERE id = 1");
            db.execute("DELETE FROM customers WHERE id = 1");

            List<CdcRow> changes = readCdcLog(db);
            assertThat(changes).hasSize(3);

            CdcRow insert = changes.get(0);
            assertThat(insert.operation).isEqualTo(CdcLog.OPERATION_CREATE);
            assertThat(insert.table).isEqualTo("customers");
            assertThat(insert.oldRow).isNull();
            assertThat(insert.newRow).contains("\"id\":1", "\"name\":\"Alice\"");

            CdcRow update = changes.get(1);
            assertThat(update.operation).isEqualTo(CdcLog.OPERATION_UPDATE);
            assertThat(update.table).isEqualTo("customers");
            assertThat(update.oldRow).contains("\"name\":\"Alice\"");
            assertThat(update.newRow).contains("\"name\":\"Alicia\"");

            CdcRow delete = changes.get(2);
            assertThat(delete.operation).isEqualTo(CdcLog.OPERATION_DELETE);
            assertThat(delete.table).isEqualTo("customers");
            assertThat(delete.oldRow).contains("\"name\":\"Alicia\"");
            assertThat(delete.newRow).isNull();
        }
    }

    /** One row of {@code _debezium_cdc_log}, in change order. */
    private record CdcRow(String table, String operation, String oldRow, String newRow) {
    }

    /** Reads every CDC log row, ordered by {@code change_id}. */
    private static List<CdcRow> readCdcLog(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.TABLE_NAME_COLUMN + ", " + CdcLog.OPERATION + ", "
                + CdcLog.OLD_ROW_DATA + ", " + CdcLog.NEW_ROW_DATA
                + " FROM " + CdcLog.TABLE_NAME + " ORDER BY " + CdcLog.CHANGE_ID;
        return db.queryAndMap(query, rs -> {
            List<CdcRow> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new CdcRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rows;
        });
    }
}
