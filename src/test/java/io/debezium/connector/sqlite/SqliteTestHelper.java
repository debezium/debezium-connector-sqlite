/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * A throwaway SQLite database for integration tests.
 *
 * <p>Creating one makes a temporary {@code .db} file, opens a {@link JdbcConnection} to it, switches
 * the database to WAL journal mode and confirms the switch took effect, then runs the
 * {@link CdcLog#CREATE_TABLE_DDL frozen CDC log DDL} so {@code _debezium_cdc_log} exists. Tests use
 * the open {@link #connection() connection} to read and write the file directly, without a running
 * change-event writer.
 *
 * <p>The helper is {@link AutoCloseable}. Closing it closes the connection and deletes the temporary
 * file and its WAL side files, so a test can use it in a try-with-resources block and leave nothing
 * behind.
 */
public final class SqliteTestHelper implements AutoCloseable {

    /** The journal mode {@code PRAGMA journal_mode=WAL} reports back once WAL is active. */
    private static final String JOURNAL_MODE_WAL = "wal";

    /** SQLite quotes identifiers with double quotes. */
    private static final String IDENTIFIER_QUOTE = "\"";

    private final Path databaseFile;
    private final JdbcConnection connection;

    private SqliteTestHelper(Path databaseFile, JdbcConnection connection) {
        this.databaseFile = databaseFile;
        this.connection = connection;
    }

    /**
     * Creates a temporary SQLite database in WAL mode with an empty {@code _debezium_cdc_log} table.
     *
     * @return an open helper the caller must close
     * @throws IOException if the temporary file cannot be created
     * @throws SQLException if the database cannot be opened or initialized
     */
    public static SqliteTestHelper create() throws IOException, SQLException {
        Path databaseFile = Testing.Files.createTestingFile("sqlite/" + UUID.randomUUID() + ".db").toPath();
        JdbcConnection connection = new JdbcConnection(
                JdbcConfiguration.empty(),
                config -> DriverManager.getConnection("jdbc:sqlite:" + databaseFile),
                IDENTIFIER_QUOTE, IDENTIFIER_QUOTE);
        try {
            connection.connect();
            enableWal(connection);
            connection.execute(CdcLog.CREATE_TABLE_DDL);
        }
        catch (SQLException | RuntimeException e) {
            connection.close();
            Files.deleteIfExists(databaseFile);
            throw e;
        }
        return new SqliteTestHelper(databaseFile, connection);
    }

    /** The open connection to the temporary database. */
    public JdbcConnection connection() {
        return connection;
    }

    /** The path of the temporary database file. */
    public Path databaseFile() {
        return databaseFile;
    }

    /**
     * Installs CDC triggers on a source table so its changes are captured into
     * {@code _debezium_cdc_log}. The table's columns are read from {@code PRAGMA table_info}, and the
     * insert, update, and delete triggers built from them are run against the database. The table
     * must already exist.
     *
     * @param table the source table to capture changes from
     * @throws SQLException if the columns cannot be read or the triggers cannot be created
     */
    public void installTriggers(String table) throws SQLException {
        List<String> columns = connection.queryAndMap("PRAGMA table_info(" + table + ")", rs -> {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString("name"));
            }
            return names;
        });
        connection.execute(TriggerGenerator.createTriggers(table, columns).toArray(new String[0]));
    }

    /**
     * Closes the connection and deletes the temporary database file along with its {@code -wal} and
     * {@code -shm} side files.
     */
    @Override
    public void close() throws SQLException, IOException {
        try {
            connection.close();
        }
        finally {
            Files.deleteIfExists(sideFile("-wal"));
            Files.deleteIfExists(sideFile("-shm"));
            Files.deleteIfExists(databaseFile);
        }
    }

    /** Switches the connection to WAL journal mode and fails if SQLite did not honor it. */
    private static void enableWal(JdbcConnection connection) throws SQLException {
        String mode = connection.queryAndMap("PRAGMA journal_mode=WAL",
                rs -> rs.next() ? rs.getString(1) : null);
        if (!JOURNAL_MODE_WAL.equalsIgnoreCase(mode)) {
            throw new IllegalStateException("Expected WAL journal mode but SQLite reported: " + mode);
        }
    }

    /** The WAL side file next to the database file, named by SQLite as {@code <db><suffix>}. */
    private Path sideFile(String suffix) {
        return databaseFile.resolveSibling(databaseFile.getFileName() + suffix);
    }
}
