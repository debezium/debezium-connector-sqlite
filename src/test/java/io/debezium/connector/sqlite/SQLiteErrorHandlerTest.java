/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

class SQLiteErrorHandlerTest {

    private final SQLiteErrorHandler handler = new SQLiteErrorHandler(null, null, null);

    @Test
    void retriesSqliteBusy() {
        assertThat(handler.isRetriable(sqliteException(SQLiteErrorCode.SQLITE_BUSY))).isTrue();
    }

    @Test
    void retriesSqliteBusySnapshot() {
        assertThat(handler.isRetriable(sqliteException(SQLiteErrorCode.SQLITE_BUSY_SNAPSHOT))).isTrue();
    }

    @Test
    void retriesSqliteLocked() {
        assertThat(handler.isRetriable(sqliteException(SQLiteErrorCode.SQLITE_LOCKED))).isTrue();
    }

    @Test
    void retriesSqliteLockedSharedCache() {
        assertThat(handler.isRetriable(sqliteException(SQLiteErrorCode.SQLITE_LOCKED_SHAREDCACHE))).isTrue();
    }

    @Test
    void doesNotRetryAConstraintViolation() {
        assertThat(handler.isRetriable(sqliteException(SQLiteErrorCode.SQLITE_CONSTRAINT))).isFalse();
    }

    @Test
    void doesNotRetryCorruption() {
        assertThat(handler.isRetriable(sqliteException(SQLiteErrorCode.SQLITE_CORRUPT))).isFalse();
    }

    @Test
    void doesNotRetryAPlainSqlException() {
        assertThat(handler.isRetriable(new SQLException("connection reset"))).isFalse();
    }

    private static SQLiteException sqliteException(SQLiteErrorCode code) {
        return new SQLiteException(code.toString(), code);
    }
}
