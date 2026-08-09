/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SQLiteVersionTest {

    private static final String MINIMUM = "3.35.0";

    @Test
    void versionsAtOrAboveMinimumAreAccepted() {
        assertThat(SQLiteVersion.isAtLeast("3.35.0", MINIMUM)).isTrue();
        assertThat(SQLiteVersion.isAtLeast("3.35.1", MINIMUM)).isTrue();
        assertThat(SQLiteVersion.isAtLeast("3.45.1", MINIMUM)).isTrue();
        assertThat(SQLiteVersion.isAtLeast("4.0.0", MINIMUM)).isTrue();
        assertThat(SQLiteVersion.isAtLeast("3.35", MINIMUM)).isTrue();
    }

    @Test
    void versionsBelowMinimumAreRejected() {
        assertThat(SQLiteVersion.isAtLeast("3.34.9", MINIMUM)).isFalse();
        assertThat(SQLiteVersion.isAtLeast("3.7.17", MINIMUM)).isFalse();
        assertThat(SQLiteVersion.isAtLeast("2.99.99", MINIMUM)).isFalse();
    }

    @Test
    void nullOrUnparseableVersionsFailClosed() {
        assertThat(SQLiteVersion.isAtLeast(null, MINIMUM)).isFalse();
        assertThat(SQLiteVersion.isAtLeast("not-a-version", MINIMUM)).isFalse();
    }
}
