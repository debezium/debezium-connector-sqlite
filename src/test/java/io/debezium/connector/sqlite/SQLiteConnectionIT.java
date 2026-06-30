/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.util.Testing;

/**
 * Integration test for the {@link SQLiteConnection} startup prerequisites against a real, freshly
 * created {@code .db} file. The connection drives the steps itself, so the file starts in SQLite's
 * default journal mode with no CDC log table.
 */
class SQLiteConnectionIT {

    private Path databaseFile;
    private SQLiteConnection connection;

    @BeforeEach
    void createDatabaseFile() {
        databaseFile = Testing.Files.createTestingFile("sqlite/" + UUID.randomUUID() + ".db").toPath();
        connection = new SQLiteConnection(databaseFile.toString());
    }

    @AfterEach
    void cleanUp() throws SQLException, IOException {
        try {
            connection.close();
        }
        finally {
            Files.deleteIfExists(sideFile("-wal"));
            Files.deleteIfExists(sideFile("-shm"));
            Files.deleteIfExists(databaseFile);
        }
    }

    @Test
    void startupSequenceEnablesWalCreatesLogTableAndPassesVersionGuard() throws SQLException {
        connection.enforceWalMode();
        connection.createCdcLogTable();
        connection.verifyMinimumVersion();

        assertThat(journalMode()).isEqualToIgnoringCase("wal");
        assertThat(cdcLogTableExists()).isTrue();
    }

    @Test
    void verifyMinimumVersionPassesAgainstABundledDriver() throws SQLException {
        // The bundled org.xerial sqlite-jdbc driver ships a modern SQLite, well above 3.35.0, so the
        // guard must not throw when run against a real connection.
        connection.verifyMinimumVersion();
    }

    @Test
    void enforceWalModeIsIdempotent() throws SQLException {
        connection.enforceWalMode();
        connection.enforceWalMode();
        assertThat(journalMode()).isEqualToIgnoringCase("wal");
    }

    @Test
    void createCdcLogTableIsIdempotent() throws SQLException {
        connection.enforceWalMode();
        connection.createCdcLogTable();
        assertThatCode(connection::createCdcLogTable).doesNotThrowAnyException();
        assertThat(cdcLogTableExists()).isTrue();
    }

    @Test
    void readMaxChangeIdReturnsZeroForEmptyLog() throws SQLException {
        connection.createCdcLogTable();

        assertThat(connection.readMaxChangeId()).isZero();
    }

    @Test
    void readMaxChangeIdReturnsLargestChangeId() throws SQLException {
        connection.createCdcLogTable();
        insertCdcRow(5);
        insertCdcRow(9);
        insertCdcRow(7);

        assertThat(connection.readMaxChangeId()).isEqualTo(9L);
    }

    private void insertCdcRow(long changeId) throws SQLException {
        connection.execute(String.format(
                "INSERT INTO %s (%s, %s, %s, %s) VALUES (%d, 'users', 'c', 0)",
                CdcLog.TABLE_NAME, CdcLog.CHANGE_ID, CdcLog.TABLE_NAME_COLUMN,
                CdcLog.OPERATION, CdcLog.COMMITTED_AT, changeId));
    }

    private String journalMode() throws SQLException {
        return connection.queryAndMap("PRAGMA journal_mode", rs -> rs.next() ? rs.getString(1) : null);
    }

    private boolean cdcLogTableExists() throws SQLException {
        return connection.queryAndMap(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='" + CdcLog.TABLE_NAME + "'",
                rs -> rs.next() && rs.getInt(1) == 1);
    }

    private Path sideFile(String suffix) {
        return databaseFile.resolveSibling(databaseFile.getFileName() + suffix);
    }
}
