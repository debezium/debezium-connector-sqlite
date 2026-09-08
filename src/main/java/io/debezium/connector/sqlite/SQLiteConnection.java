/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.ColumnEditor;

/**
 * A JDBC connection to a single SQLite database file. SQLite reaches its database through a file path
 * rather than a host and port, so the URL is built from the path and the inherited relational
 * connection fields are unused.
 */
public class SQLiteConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteConnection.class);

    /** JDBC URL prefix; the database file path is appended. */
    private static final String URL_PREFIX = "jdbc:sqlite:";

    /** SQLite quotes identifiers with double quotes. */
    private static final String IDENTIFIER_QUOTE = "\"";

    /** The mode {@code PRAGMA journal_mode} reports while WAL is active. */
    public static final String JOURNAL_MODE_WAL = "wal";

    /**
     * The connector requires SQLite 3.35.0 or later. That release added
     * {@code ALTER TABLE DROP COLUMN}, the last schema-change capability the connector must handle.
     */
    static final String MINIMUM_VERSION = "3.35.0";

    public SQLiteConnection(String databaseFilePath) {
        super(JdbcConfiguration.empty(),
                config -> DriverManager.getConnection(URL_PREFIX + databaseFilePath),
                IDENTIFIER_QUOTE, IDENTIFIER_QUOTE);
    }

    /**
     * Switches the database to WAL journal mode and confirms SQLite honored it: when another
     * connection holds an exclusive lock the pragma silently leaves the mode unchanged.
     */
    public void enforceWalMode() {
        String mode = guarded("Failed to set WAL journal mode",
                () -> queryAndMap("PRAGMA journal_mode=WAL", rs -> rs.next() ? rs.getString(1) : null));
        if (!JOURNAL_MODE_WAL.equalsIgnoreCase(mode)) {
            throw new DebeziumException("Failed to set WAL journal mode (SQLite reported '" + mode
                    + "'). Another connection may hold an exclusive lock on the database file.");
        }
    }

    /** Reads the current journal mode without changing it, or null if the pragma returns no row. */
    public String journalMode() {
        return guarded("Failed to read the journal mode",
                () -> queryAndMap("PRAGMA journal_mode", rs -> rs.next() ? rs.getString(1) : null));
    }

    /** Creates the {@code _debezium_cdc_log} table if it does not exist. */
    public void createCdcLogTable() {
        guarded("Failed to create the CDC log table", () -> execute(CdcLog.CREATE_TABLE_DDL));
    }

    /** The largest {@code change_id} in {@code _debezium_cdc_log}, or 0 for an empty log; the resume point. */
    public long readMaxChangeId() {
        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", CdcLog.CHANGE_ID, CdcLog.TABLE_NAME);
        return guarded("Failed to read the maximum change id",
                () -> queryAndMap(sql, rs -> rs.next() ? rs.getLong(1) : 0L));
    }

    /**
     * Reads the next batch of change rows after {@code afterChangeId} (exclusive), in {@code change_id}
     * order, at most {@code limit} rows. Each call is a short autocommit read, so it does not hold a
     * read transaction open across the inter-poll sleep and block WAL checkpointing.
     */
    public List<CdcLogRow> readChanges(long afterChangeId, int limit) throws SQLException {
        String sql = String.format(
                "SELECT %s, %s, %s, %s, %s, %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT %d",
                CdcLog.CHANGE_ID, CdcLog.TABLE_NAME_COLUMN, CdcLog.OPERATION,
                CdcLog.OLD_ROW_DATA, CdcLog.NEW_ROW_DATA, CdcLog.COMMITTED_AT,
                CdcLog.TABLE_NAME, CdcLog.CHANGE_ID, CdcLog.CHANGE_ID, limit);
        return prepareQueryAndMap(sql,
                statement -> statement.setLong(1, afterChangeId),
                resultSet -> {
                    List<CdcLogRow> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        rows.add(new CdcLogRow(
                                resultSet.getLong(1),
                                resultSet.getString(2),
                                resultSet.getString(3),
                                resultSet.getString(4),
                                resultSet.getString(5),
                                resultSet.getLong(6)));
                    }
                    return rows;
                });
    }

    /**
     * Corrects a column's JDBC type to the one its SQLite affinity implies, since the driver reports a
     * type that ignores affinity. Defensive: the schema builder and value converter resolve affinity
     * from the declared type directly, so they do not rely on this.
     */
    @Override
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        return column.jdbcType(SQLiteTypeAffinity.of(column.typeName()).jdbcType());
    }

    /** Fails if the database's SQLite version is below {@link #MINIMUM_VERSION}. */
    public void verifyMinimumVersion() {
        String version = guarded("Failed to read the SQLite version",
                () -> queryAndMap("SELECT sqlite_version()", rs -> rs.next() ? rs.getString(1) : null));
        if (!SQLiteVersion.isAtLeast(version, MINIMUM_VERSION)) {
            throw new DebeziumException("SQLite " + MINIMUM_VERSION + " or later is required, but the "
                    + "database reports '" + version + "'.");
        }
        LOGGER.info("SQLite version {} meets the minimum required {}", version, MINIMUM_VERSION);
    }

    /** A JDBC call that yields a value and may fail with a checked {@link SQLException}. */
    @FunctionalInterface
    private interface JdbcCall<T> {
        T call() throws SQLException;
    }

    /** Runs a JDBC call, rethrowing any {@link SQLException} as a {@link DebeziumException}. */
    private static <T> T guarded(String failureMessage, JdbcCall<T> call) {
        try {
            return call.call();
        }
        catch (SQLException e) {
            throw new DebeziumException(failureMessage, e);
        }
    }
}
