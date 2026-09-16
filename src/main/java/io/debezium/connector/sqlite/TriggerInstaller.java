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
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/**
 * Installs the CDC capture triggers on a source table. {@link #install(JdbcConnection, String)} reads
 * the table's columns from {@code PRAGMA table_info} and runs the statements {@link TriggerGenerator}
 * builds from them. The table must already exist.
 */
public final class TriggerInstaller {

    private TriggerInstaller() {
    }

    /**
     * Installs the triggers on every captured table. The table list comes from the database, not the
     * connector schema, and each table is kept only if the filter includes it. Installation is idempotent.
     */
    public static void installAll(JdbcConnection connection, TableFilter tableFilter) throws SQLException {
        for (TableId tableId : connection.getAllTableIds(null)) {
            if (tableFilter.isIncluded(tableId)) {
                install(connection, tableId.table());
            }
        }
    }

    /** Installs the insert, update, and delete triggers on a source table, which must already exist. */
    public static void install(JdbcConnection connection, String table) throws SQLException {
        List<String> columns = readColumnNames(connection, table);
        connection.execute(TriggerGenerator.createTriggers(table, columns).toArray(new String[0]));
    }

    /**
     * Rebuilds a table's triggers to match its current columns, dropping the three and creating them again
     * so a trigger left stale by an {@code ALTER TABLE} is replaced. {@code CREATE TRIGGER IF NOT EXISTS}
     * alone cannot, so the drop is required; both run in one transaction, so no write is captured by a
     * missing trigger in between.
     */
    public static void rebuild(JdbcConnection connection, String table) throws SQLException {
        List<String> columns = readColumnNames(connection, table);
        List<String> statements = new ArrayList<>(TriggerGenerator.dropTriggers(table));
        statements.addAll(TriggerGenerator.createTriggers(table, columns));
        runInTransaction(connection, statements);
    }

    /**
     * Drops a table's three capture triggers if they exist, to remove triggers left on a table the
     * connector no longer captures, such as after an {@code ALTER TABLE ... RENAME TO} where the renamed
     * table keeps its old triggers and they would otherwise keep firing.
     */
    public static void drop(JdbcConnection connection, String table) throws SQLException {
        connection.execute(TriggerGenerator.dropTriggers(table).toArray(new String[0]));
    }

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

    static List<String> readColumnNames(JdbcConnection connection, String table) throws SQLException {
        return connection.queryAndMap("PRAGMA table_info(" + table + ")", rs -> {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString("name"));
            }
            return names;
        });
    }
}
