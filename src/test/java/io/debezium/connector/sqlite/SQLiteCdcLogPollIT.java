/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;

/**
 * Integration test for {@link SQLiteConnection#readChanges}, the bounded poll query the streaming source
 * runs. It writes several rows through the capture triggers, then reads the {@code _debezium_cdc_log}
 * back with a second connection and checks the cursor, the ascending order, and the batch bound against a
 * real temp database.
 */
class SQLiteCdcLogPollIT {

    @Test
    void readsChangesAfterACursorInOrderBoundedByTheLimit() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)");
            helper.installTriggers("products");

            db.execute("INSERT INTO products (id, name) VALUES (1, 'a')");
            db.execute("INSERT INTO products (id, name) VALUES (2, 'b')");
            db.execute("INSERT INTO products (id, name) VALUES (3, 'c')");

            try (SQLiteConnection reader = new SQLiteConnection(helper.databaseFile().toString())) {
                List<CdcLogRow> all = reader.readChanges(0, 100);
                assertThat(all).extracting(CdcLogRow::changeId).containsExactly(1L, 2L, 3L);
                assertThat(all).extracting(CdcLogRow::operation).containsOnly(CdcLog.OPERATION_CREATE);
                assertThat(all.get(0).newRowData()).contains("\"name\":\"a\"");

                // The cursor is exclusive, so a read after change_id 2 returns only the last row.
                assertThat(reader.readChanges(2, 100)).extracting(CdcLogRow::changeId).containsExactly(3L);

                // The limit bounds the batch to the first two rows.
                assertThat(reader.readChanges(0, 2)).extracting(CdcLogRow::changeId).containsExactly(1L, 2L);

                // Nothing remains after the last change_id.
                assertThat(reader.readChanges(3, 100)).isEmpty();
            }
        }
    }
}
