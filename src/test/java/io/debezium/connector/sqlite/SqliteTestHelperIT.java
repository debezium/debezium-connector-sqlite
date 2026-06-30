/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * Self-test for {@link SqliteTestHelper}. Confirms that a freshly created helper gives a real
 * on-disk database in WAL mode with an empty {@code _debezium_cdc_log} table, and that closing it
 * removes the temporary file. Uses a temp {@code .db} rather than an in-memory database, so it runs
 * under Failsafe as an integration test.
 */
public class SqliteTestHelperIT {

    @Test
    void createsWalDatabaseWithEmptyCdcLog() throws Exception {
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            assertThat(Files.exists(helper.databaseFile())).isTrue();

            String journalMode = helper.connection().queryAndMap(
                    "PRAGMA journal_mode", rs -> rs.next() ? rs.getString(1) : null);
            assertThat(journalMode).isEqualToIgnoringCase("wal");

            long rowCount = helper.connection().queryAndMap(
                    "SELECT COUNT(*) FROM " + CdcLog.TABLE_NAME, rs -> {
                        rs.next();
                        return rs.getLong(1);
                    });
            assertThat(rowCount).isZero();
        }
    }

    @Test
    void deletesTemporaryFileOnClose() throws Exception {
        Path databaseFile;
        try (SqliteTestHelper helper = SqliteTestHelper.create()) {
            databaseFile = helper.databaseFile();
            assertThat(Files.exists(databaseFile)).isTrue();
        }
        assertThat(Files.exists(databaseFile)).isFalse();
    }
}
