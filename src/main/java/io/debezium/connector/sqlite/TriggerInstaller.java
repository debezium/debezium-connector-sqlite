/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.debezium.jdbc.JdbcConnection;

/**
 * Installs the CDC capture triggers on a source table. {@link #install(JdbcConnection, String)} reads
 * the table's columns from {@code PRAGMA table_info} and runs the statements {@link TriggerGenerator}
 * builds from them. The table must already exist.
 */
public final class TriggerInstaller {

    private TriggerInstaller() {
    }

    /** Installs the insert, update, and delete triggers on a source table, which must already exist. */
    public static void install(JdbcConnection connection, String table) throws SQLException {
        List<String> columns = readColumnNames(connection, table);
        connection.execute(TriggerGenerator.createTriggers(table, columns).toArray(new String[0]));
    }

    private static List<String> readColumnNames(JdbcConnection connection, String table) throws SQLException {
        return connection.queryAndMap("PRAGMA table_info(" + table + ")", rs -> {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString("name"));
            }
            return names;
        });
    }
}
