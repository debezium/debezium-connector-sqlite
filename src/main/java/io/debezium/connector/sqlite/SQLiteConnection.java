/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A JDBC connection to a single SQLite database file.
 *
 * <p>This class owns the connection concerns the task drives at startup: it builds the
 * {@code jdbc:sqlite:<path>} URL, switches the file to WAL journal mode and verifies the switch took
 * effect, creates the {@code _debezium_cdc_log} table, and confirms the SQLite version is recent
 * enough. SQLite reaches its database through a file path rather than a host and port, so the
 * inherited relational connection fields are unused and the URL is built from the file path alone.
 */
public class SQLiteConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteConnection.class);

    /** JDBC URL prefix for the SQLite driver; the database file path is appended to it. */
    private static final String URL_PREFIX = "jdbc:sqlite:";

    /** SQLite quotes identifiers with double quotes. */
    private static final String IDENTIFIER_QUOTE = "\"";

    /** The mode {@code PRAGMA journal_mode=WAL} reports back once WAL is active. */
    private static final String JOURNAL_MODE_WAL = "wal";

    /**
     * The connector requires SQLite 3.35.0 or later. That release added
     * {@code ALTER TABLE DROP COLUMN}, the last schema-change capability the connector must handle.
     */
    static final String MINIMUM_VERSION = "3.35.0";

    /** {@link #MINIMUM_VERSION} split into its numeric major, minor, and patch parts for comparison. */
    private static final int[] MINIMUM_VERSION_PARTS = parseVersionParts(MINIMUM_VERSION);

    /**
     * Opens a connection to the SQLite database at the given file path.
     *
     * @param databaseFilePath the path to the {@code .db} file the connector monitors
     */
    public SQLiteConnection(String databaseFilePath) {
        super(JdbcConfiguration.empty(),
                config -> DriverManager.getConnection(URL_PREFIX + databaseFilePath),
                IDENTIFIER_QUOTE, IDENTIFIER_QUOTE);
    }

    /**
     * Switches the database to WAL journal mode and confirms SQLite honored the switch. The check
     * reads the mode the pragma actually returned rather than trusting that it ran, because when
     * another connection holds an exclusive lock the pragma silently leaves the mode unchanged.
     *
     * @throws DebeziumException if SQLite did not switch to WAL mode
     * @throws SQLException if the pragma cannot be run
     */
    public void enforceWalMode() throws SQLException {
        String mode = queryAndMap("PRAGMA journal_mode=WAL", rs -> rs.next() ? rs.getString(1) : null);
        if (!JOURNAL_MODE_WAL.equalsIgnoreCase(mode)) {
            throw new DebeziumException("Failed to set WAL journal mode (SQLite reported '" + mode
                    + "'). Another connection may hold an exclusive lock on the database file.");
        }
    }

    /**
     * Creates the {@code _debezium_cdc_log} table if it does not already exist, using the frozen DDL
     * shared with the rest of the connector.
     *
     * @throws SQLException if the table cannot be created
     */
    public void createCdcLogTable() throws SQLException {
        execute(CdcLog.CREATE_TABLE_DDL);
    }

    /**
     * Reads the SQLite version reported by the driver and fails if it is below
     * {@link #MINIMUM_VERSION}.
     *
     * @throws DebeziumException if the database version is too old
     * @throws SQLException if the version cannot be read
     */
    public void verifyMinimumVersion() throws SQLException {
        String version = queryAndMap("SELECT sqlite_version()", rs -> rs.next() ? rs.getString(1) : null);
        if (!isAtLeastMinimumVersion(version)) {
            throw new DebeziumException("SQLite " + MINIMUM_VERSION + " or later is required, but the "
                    + "database reports '" + version + "'.");
        }
        LOGGER.info("SQLite version {} meets the minimum required {}", version, MINIMUM_VERSION);
    }

    /**
     * Returns whether an {@code X.Y.Z} version string is at or above {@link #MINIMUM_VERSION}. A
     * null or unparseable version is treated as below the minimum so the guard fails closed.
     *
     * @param version the version string reported by {@code sqlite_version()}
     * @return true if the version is at least the minimum, false otherwise
     */
    static boolean isAtLeastMinimumVersion(String version) {
        if (version == null) {
            return false;
        }
        int[] parts = parseVersionParts(version);
        for (int i = 0; i < MINIMUM_VERSION_PARTS.length; i++) {
            int part = i < parts.length ? parts[i] : 0;
            if (part != MINIMUM_VERSION_PARTS[i]) {
                return part > MINIMUM_VERSION_PARTS[i];
            }
        }
        return true;
    }

    /**
     * Splits an {@code X.Y.Z} version string into its numeric parts, parsing the leading digits of
     * each dot-separated token.
     */
    private static int[] parseVersionParts(String version) {
        String[] tokens = version.trim().split("\\.");
        int[] parts = new int[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            parts[i] = parseLeadingInt(tokens[i]);
        }
        return parts;
    }

    /**
     * Parses the leading run of digits in a version token, returning -1 if the token does not start
     * with a digit. Comparing -1 against any minimum part fails the version guard.
     */
    private static int parseLeadingInt(String token) {
        int end = 0;
        while (end < token.length() && Character.isDigit(token.charAt(end))) {
            end++;
        }
        return end == 0 ? -1 : Integer.parseInt(token.substring(0, end));
    }
}
