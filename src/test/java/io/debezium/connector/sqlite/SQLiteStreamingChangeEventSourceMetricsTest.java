/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;

class SQLiteStreamingChangeEventSourceMetricsTest {

    private SQLiteStreamingChangeEventSourceMetrics metrics;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.from(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "/tmp/metrics-unit-test.db",
                "topic.prefix", "sqlite-metrics-test"));
        SQLiteConnectorConfig connectorConfig = new SQLiteConnectorConfig(configuration);
        CdcSourceTaskContext<SQLiteConnectorConfig> taskContext = new CdcSourceTaskContext<>(
                configuration, connectorConfig, Map.of());
        CapturedTablesSupplier capturedTablesSupplier = () -> List.of();

        metrics = new SQLiteStreamingChangeEventSourceMetrics(
                taskContext, null, new SQLiteEventMetadataProvider(), capturedTablesSupplier);
    }

    @Test
    void reportsZeroBeforeAnyUpdate() {
        assertThat(metrics.getCurrentChangeId()).isZero();
        assertThat(metrics.getCommittedChangeId()).isZero();
        assertThat(metrics.getCdcLogDepth()).isZero();
    }

    @Test
    void reportsTheValuesTheSourceRecords() {
        metrics.setCurrentChangeId(42L);
        metrics.setCommittedChangeId(30L);
        metrics.setCdcLogDepth(12L);

        assertThat(metrics.getCurrentChangeId()).isEqualTo(42L);
        assertThat(metrics.getCommittedChangeId()).isEqualTo(30L);
        assertThat(metrics.getCdcLogDepth()).isEqualTo(12L);
    }

    @Test
    void resetClearsTheSqliteAttributes() {
        metrics.setCurrentChangeId(42L);
        metrics.setCommittedChangeId(30L);
        metrics.setCdcLogDepth(12L);

        metrics.reset();

        assertThat(metrics.getCurrentChangeId()).isZero();
        assertThat(metrics.getCommittedChangeId()).isZero();
        assertThat(metrics.getCdcLogDepth()).isZero();
    }
}
