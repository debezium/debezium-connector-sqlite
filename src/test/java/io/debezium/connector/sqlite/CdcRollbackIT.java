/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * Proves a rolled-back write is never listed in {@code _debezium_cdc_log}.
 * The capture trigger inserts its log row inside the writer's own transaction,
 * so while the transaction is open and not committed, no other connection can
 * see its rows (due to WAL snapshot isolation enabled by {@code SqliteTestHelper.create()}).
 * Rolling the transaction back then undoes the log rows along with the data change.
 * It uses a real temp database with two connections, so it runs under Failsafe.
 */
public class CdcRollbackIT {

    @Test
    @FixFor("debezium/dbz#2933")
    void rolledBackTransactionIsNeverListedInTheCdcLog() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create();
                SQLiteConnection writer = new SQLiteConnection(helper.databaseFile().toString())) {
            JdbcConnection db = helper.connection();
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
            helper.installTriggers("customers");
            writer.connect();
            writer.connection().setAutoCommit(false);

            try (Statement statement = writer.connection().createStatement()) {
                statement.executeUpdate("INSERT INTO customers (id, name) VALUES (1, 'Alice')");
                statement.executeUpdate("UPDATE customers SET name = 'Alicia' WHERE id = 1");
            }

            assertThat(readCdcOperations(writer)).containsExactly(
                    CdcLog.OPERATION_CREATE,
                    CdcLog.OPERATION_UPDATE);

            assertThat(readCustomerIds(writer)).containsExactly(1L);
            assertThat(readCdcOperations(db)).isEmpty();
            assertThat(readCustomerIds(db)).isEmpty();

            writer.connection().rollback();

            assertThat(readCdcOperations(writer)).isEmpty();
            assertThat(readCustomerIds(writer)).isEmpty();

            try (Statement statement = writer.connection().createStatement()) {
                statement.executeUpdate("INSERT INTO customers (id, name) VALUES (2, 'Carol')");
            }
            assertThat(readCdcOperations(writer)).containsExactly(CdcLog.OPERATION_CREATE);
            assertThat(readCustomerIds(writer)).containsExactly(2L);

            assertThat(readCustomerIds(db)).isEmpty();

            writer.connection().commit();

            assertThat(readCdcOperations(db)).containsExactly(CdcLog.OPERATION_CREATE);
            assertThat(readCustomerIds(db)).containsExactly(2L);
        }
    }

    /** The operation code of every CDC log row, ordered by {@code change_id}. */
    private static List<String> readCdcOperations(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.OPERATION + " FROM " + CdcLog.TABLE_NAME
                + " ORDER BY " + CdcLog.CHANGE_ID;
        return db.queryAndMap(query, rs -> {
            List<String> operations = new ArrayList<>();
            while (rs.next()) {
                operations.add(rs.getString(1));
            }
            return operations;
        });
    }

    /** The ids of every row in the source table, ordered by id. */
    private static List<Long> readCustomerIds(JdbcConnection db) throws SQLException {
        return db.queryAndMap("SELECT id FROM customers ORDER BY id", rs -> {
            List<Long> ids = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
            return ids;
        });
    }
}
