/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlite.SQLiteConnectorConfig.SnapshotMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

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
    void databaseFilePathIsRequired() {
        Map<String, ConfigValue> results = Configuration.from(Map.of(CommonConnectorConfig.TOPIC_PREFIX.name(), "test"))
                .validate(SQLiteConnectorConfig.ALL_FIELDS);
        assertThat(results.get(SQLiteConnectorConfig.DATABASE_FILE.name()).errorMessages()).isNotEmpty();
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
        assertThat(SnapshotMode.parse("always")).isEqualTo(SnapshotMode.ALWAYS);
        assertThat(SnapshotMode.parse("no_data")).isEqualTo(SnapshotMode.NO_DATA);
        assertThat(SnapshotMode.parse("initial_only")).isEqualTo(SnapshotMode.INITIAL_ONLY);
        assertThat(SnapshotMode.parse("INITIAL")).isEqualTo(SnapshotMode.INITIAL);
        assertThatThrownBy(() -> SnapshotMode.parse("bogus"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static SQLiteConnectorConfig configWith(Map<String, String> extra) {
        Map<String, String> props = new java.util.HashMap<>(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "/tmp/test.db",
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test"));
        props.putAll(extra);
        return new SQLiteConnectorConfig(Configuration.from(props));
    }

    @Test
    void nonNullPlaceholderIsOffByDefault() {
        assertThat(configWith(Map.of()).shouldSubstituteNonNullPlaceholder()).isFalse();
    }

    @Test
    void nonNullPlaceholderAppliesUnderWarnAndSkipButNotFail() {
        // The placeholder rescues the warn and skip dead-end; under fail the connector is meant to stop.
        assertThat(configWith(Map.of("nonnull.affinity.mismatch.fallback", "true")).shouldSubstituteNonNullPlaceholder()).isTrue();
        assertThat(configWith(Map.of(
                "nonnull.affinity.mismatch.fallback", "true",
                "event.converting.failure.handling.mode", "skip")).shouldSubstituteNonNullPlaceholder()).isTrue();
        assertThat(configWith(Map.of(
                "nonnull.affinity.mismatch.fallback", "true",
                "event.converting.failure.handling.mode", "fail")).shouldSubstituteNonNullPlaceholder()).isFalse();
    }

    private static boolean isMonitored(String table) {
        Configuration config = Configuration.from(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "/tmp/test.db",
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test"));
        return new SQLiteConnectorConfig(config).getTableFilters().dataCollectionFilter()
                .isIncluded(new TableId(null, null, table));
    }

    @Test
    void monitorsUserTables() {
        assertThat(isMonitored("orders")).isTrue();
    }

    @Test
    void alwaysExcludesSqliteInternalTables() {
        assertThat(isMonitored("sqlite_sequence")).isFalse();
        assertThat(isMonitored("sqlite_master")).isFalse();
    }

    @Test
    void alwaysExcludesEveryDebeziumPrefixedTable() {
        // The always-exclude is the _debezium_ prefix, not just the cdc log name, so any internal
        // table the connector might add is excluded too.
        assertThat(isMonitored("_debezium_cdc_log")).isFalse();
        assertThat(isMonitored("_debezium_signal")).isFalse();
    }
}
