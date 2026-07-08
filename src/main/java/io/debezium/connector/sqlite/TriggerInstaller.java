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
 * Installs the CDC capture triggers on a source table.
 *
 * <p>{@link #install(JdbcConnection, String)} reads the table's columns from {@code PRAGMA table_info}
 * and runs the {@code AFTER INSERT}, {@code AFTER UPDATE}, and {@code AFTER DELETE} statements that
 * {@link TriggerGenerator} builds from them, so every change to the table is captured into
 * {@link CdcLog#TABLE_NAME _debezium_cdc_log}. {@link TriggerGenerator} builds the SQL; this class runs
 * it against the database. The table must already exist.
 */
public final class TriggerInstaller {

    private TriggerInstaller() {
    }

    /**
     * Installs the insert, update, and delete triggers on a source table.
     *
     * @param connection an open connection to the SQLite database
     * @param table the source table to capture changes from; it must already exist
     * @throws SQLException if the columns cannot be read or the triggers cannot be created
     */
    public static void install(JdbcConnection connection, String table) throws SQLException {
        List<String> columns = readColumnNames(connection, table);
        connection.execute(TriggerGenerator.createTriggers(table, columns).toArray(new String[0]));
    }

    /** Reads a table's column names in definition order from {@code PRAGMA table_info}. */
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
