/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * Verifies that the frozen {@code _debezium_cdc_log} DDL in {@link CdcLog} actually creates a table
 * matching the contract: the expected columns, types, {@code NOT NULL} flags, and an autoincrement
 * primary key. Runs against an in-memory SQLite database, so it needs no temp file and stays a
 * Surefire unit test.
 */
public class CdcLogTest {

    @Test
    void ddlCreatesTableMatchingContract() throws Exception {
        try (JdbcConnection connection = memoryConnection()) {
            connection.execute(CdcLog.CREATE_TABLE_DDL);

            assertThat(columnTypes(connection)).containsExactly(
                    Map.entry(CdcLog.CHANGE_ID, "INTEGER"),
                    Map.entry(CdcLog.TABLE_NAME_COLUMN, "TEXT"),
                    Map.entry(CdcLog.OPERATION, "TEXT"),
                    Map.entry(CdcLog.OLD_ROW_DATA, "TEXT"),
                    Map.entry(CdcLog.NEW_ROW_DATA, "TEXT"),
                    Map.entry(CdcLog.COMMITTED_AT, "INTEGER"));

            assertThat(notNullColumns(connection)).containsExactly(
                    CdcLog.TABLE_NAME_COLUMN, CdcLog.OPERATION, CdcLog.COMMITTED_AT);

            assertThat(primaryKey(connection)).isEqualTo(CdcLog.CHANGE_ID);
            assertThat(storedDdl(connection)).containsIgnoringCase("AUTOINCREMENT");
        }
    }

    /** Column name to declared type, in table order, read from {@code PRAGMA table_info}. */
    private static Map<String, String> columnTypes(JdbcConnection connection) throws Exception {
        return connection.queryAndMap("PRAGMA table_info(" + CdcLog.TABLE_NAME + ")", rs -> {
            Map<String, String> columns = new LinkedHashMap<>();
            while (rs.next()) {
                columns.put(rs.getString("name"), rs.getString("type"));
            }
            return columns;
        });
    }

    /** Names of columns declared {@code NOT NULL}, in table order. */
    private static List<String> notNullColumns(JdbcConnection connection) throws Exception {
        return connection.queryAndMap("PRAGMA table_info(" + CdcLog.TABLE_NAME + ")", rs -> {
            List<String> columns = new ArrayList<>();
            while (rs.next()) {
                if (rs.getInt("notnull") == 1) {
                    columns.add(rs.getString("name"));
                }
            }
            return columns;
        });
    }

    /** The single primary-key column. */
    private static String primaryKey(JdbcConnection connection) throws Exception {
        return connection.queryAndMap("PRAGMA table_info(" + CdcLog.TABLE_NAME + ")", rs -> {
            while (rs.next()) {
                if (rs.getInt("pk") == 1) {
                    return rs.getString("name");
                }
            }
            return null;
        });
    }

    /** The {@code CREATE TABLE} text SQLite stored, used to confirm AUTOINCREMENT survived. */
    private static String storedDdl(JdbcConnection connection) throws Exception {
        return connection.queryAndMap(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='" + CdcLog.TABLE_NAME + "'", rs -> {
                    rs.next();
                    return rs.getString("sql");
                });
    }

    /** A connection to a fresh in-memory SQLite database, reused for the life of the connection. */
    private static JdbcConnection memoryConnection() {
        return new JdbcConnection(
                JdbcConfiguration.empty(),
                config -> DriverManager.getConnection("jdbc:sqlite::memory:"),
                "\"", "\"");
    }
}
