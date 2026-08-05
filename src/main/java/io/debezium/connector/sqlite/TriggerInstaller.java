/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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

    /**
     * Rebuilds a table's triggers to match its current columns. It drops the three triggers and creates
     * them again from the live column list, so a trigger left stale by an {@code ALTER TABLE} is
     * replaced. A plain {@link #install} cannot do this: {@code CREATE TRIGGER IF NOT EXISTS} is a no-op
     * against a trigger that already exists, so the drop is required. The drop and the create run in one
     * transaction, so no write is captured by a missing trigger in between.
     *
     * @param connection an open connection to the SQLite database
     * @param table the source table whose triggers to rebuild; it must already exist
     * @throws SQLException if the columns cannot be read or the triggers cannot be replaced
     */
    public static void rebuild(JdbcConnection connection, String table) throws SQLException {
        List<String> columns = readColumnNames(connection, table);
        List<String> statements = new ArrayList<>(TriggerGenerator.dropTriggers(table));
        statements.addAll(TriggerGenerator.createTriggers(table, columns));
        runInTransaction(connection, statements);
    }

    /** Runs the statements in a single transaction, restoring the connection's autocommit mode after. */
    private static void runInTransaction(JdbcConnection connection, List<String> statements) throws SQLException {
        Connection jdbc = connection.connection();
        boolean autoCommit = jdbc.getAutoCommit();
        jdbc.setAutoCommit(false);
        try (Statement statement = jdbc.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
            jdbc.commit();
        }
        catch (SQLException e) {
            jdbc.rollback();
            throw e;
        }
        finally {
            jdbc.setAutoCommit(autoCommit);
        }
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
