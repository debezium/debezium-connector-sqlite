/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.sqlite.SQLiteConnectorConfig.SnapshotMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

class SQLiteConnectorConfigTest {

    private static final Set<String> ALL_FIELD_NAMES = SQLiteConnectorConfig.ALL_FIELDS.allFieldNames();

    @Test
    void databaseFilePathIsAccepted() {
        assertThat(ALL_FIELD_NAMES).contains(SQLiteConnectorConfig.DATABASE_FILE.name());
        assertThat(SQLiteConnectorConfig.DATABASE_FILE.name()).isEqualTo("database.file.path");
    }

    @Test
    void topicPrefixIsAccepted() {
        assertThat(ALL_FIELD_NAMES).contains(CommonConnectorConfig.TOPIC_PREFIX.name());
    }

    @Test
    void cdcLogBatchSizeDefaultsTo1000() {
        assertThat(ALL_FIELD_NAMES).contains("cdc.log.batch.size");
        assertThat(SQLiteConnectorConfig.CDC_LOG_BATCH_SIZE.defaultValue()).isEqualTo(1000);
    }

    @Test
    void pollIntervalIsInheritedWithDefault500() {
        // poll.interval.ms is provided by CommonConnectorConfig; the connector reuses it rather than
        // declaring its own, so it appears exactly once in the field set with the standard default.
        assertThat(ALL_FIELD_NAMES).contains(CommonConnectorConfig.POLL_INTERVAL_MS.name());
        assertThat(CommonConnectorConfig.POLL_INTERVAL_MS.defaultValue()).isEqualTo(500L);
    }

    @Test
    void inheritedJdbcConnectionFieldsAreAbsent() {
        // SQLite reaches its database through database.file.path, so the relational host/port/user/
        // password fields must not be part of the validated surface.
        assertThat(ALL_FIELD_NAMES).doesNotContain(
                RelationalDatabaseConnectorConfig.HOSTNAME.name(),
                RelationalDatabaseConnectorConfig.PORT.name(),
                RelationalDatabaseConnectorConfig.USER.name(),
                RelationalDatabaseConnectorConfig.PASSWORD.name());
    }

    @Test
    void snapshotModeParseRoundTrips() {
        assertThat(SnapshotMode.parse("initial")).isEqualTo(SnapshotMode.INITIAL);
        assertThat(SnapshotMode.parse("no_data")).isEqualTo(SnapshotMode.NO_DATA);
        assertThat(SnapshotMode.parse("INITIAL")).isEqualTo(SnapshotMode.INITIAL);
        assertThatThrownBy(() -> SnapshotMode.parse("bogus"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
