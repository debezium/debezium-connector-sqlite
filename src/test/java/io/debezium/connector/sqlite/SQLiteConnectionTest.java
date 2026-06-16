/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SQLiteConnectionTest {

    @Test
    void versionsAtOrAboveMinimumAreAccepted() {
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("3.35.0")).isTrue();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("3.35.1")).isTrue();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("3.45.1")).isTrue();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("4.0.0")).isTrue();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("3.35")).isTrue();
    }

    @Test
    void versionsBelowMinimumAreRejected() {
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("3.34.9")).isFalse();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("3.7.17")).isFalse();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("2.99.99")).isFalse();
    }

    @Test
    void nullOrUnparseableVersionsFailClosed() {
        assertThat(SQLiteConnection.isAtLeastMinimumVersion(null)).isFalse();
        assertThat(SQLiteConnection.isAtLeastMinimumVersion("not-a-version")).isFalse();
    }
}
