/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;

/**
 * Integration tests for {@link TriggerInstaller}'s rebuild and drop paths against a real temp database.
 * These exercise what happens to a table's capture triggers after its columns change under them.
 */
public class TriggerInstallerIT {

    @Test
    void reinstallAfterAddColumnDoesNotCaptureNewColumn() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
            TriggerInstaller.install(db, "orders");

            db.execute("ALTER TABLE orders ADD COLUMN discount REAL");
            // CREATE TRIGGER IF NOT EXISTS is a no-op against the existing trigger, so the stale trigger
            // stays and the new column is not captured.
            TriggerInstaller.install(db, "orders");
            db.execute("INSERT INTO orders (id, name, discount) VALUES (1, 'a', 5.0)");

            assertThat(latestNewRow(db)).doesNotContain("discount");
        }
    }

    @Test
    void rebuildAfterAddColumnCapturesNewColumn() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
            TriggerInstaller.install(db, "orders");

            db.execute("ALTER TABLE orders ADD COLUMN discount REAL");
            TriggerInstaller.rebuild(db, "orders");
            db.execute("INSERT INTO orders (id, name, discount) VALUES (2, 'b', 7.5)");

            assertThat(latestNewRow(db)).contains("\"discount\":7.5");
        }
    }

    @Test
    void dropRemovesTheTriggersAndStopsCapture() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)");
            TriggerInstaller.install(db, "orders");

            TriggerInstaller.drop(db, "orders");

            assertThat(triggerCount(db)).isZero();
            db.execute("INSERT INTO orders (id, name) VALUES (1, 'a')");
            assertThat(cdcRowCount(db)).isZero();
        }
    }

    /** The {@code new_row_data} of the most recent CDC log row. */
    private static String latestNewRow(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.NEW_ROW_DATA + " FROM " + CdcLog.TABLE_NAME
                + " ORDER BY " + CdcLog.CHANGE_ID + " DESC LIMIT 1";
        return db.queryAndMap(query, rs -> rs.next() ? rs.getString(1) : null);
    }

    private static int triggerCount(JdbcConnection db) throws SQLException {
        return db.queryAndMap("SELECT count(*) FROM sqlite_master WHERE type='trigger'",
                rs -> rs.next() ? rs.getInt(1) : 0);
    }

    private static int cdcRowCount(JdbcConnection db) throws SQLException {
        return db.queryAndMap("SELECT count(*) FROM " + CdcLog.TABLE_NAME, rs -> rs.next() ? rs.getInt(1) : 0);
    }
}
