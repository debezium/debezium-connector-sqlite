/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlite.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;

/**
 * Unit tests for {@link SQLiteSnapshotChangeEventSource} that need no database. The snapshot SELECT is
 * delegated to the snapshotter service's query, so the source must hand the table and its columns to
 * {@link SelectAllSnapshotQuery} and return the statement it builds.
 */
class SQLiteSnapshotChangeEventSourceTest {

    private static SQLiteSnapshotChangeEventSource newSource() {
        Configuration config = Configuration.from(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "test.db",
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test"));
        SQLiteConnectorConfig connectorConfig = new SQLiteConnectorConfig(config);
        SnapshotterService snapshotterService = new SnapshotterService(null, new SelectAllSnapshotQuery(), null);
        MainConnectionProvidingConnectionFactory<SQLiteConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new SQLiteConnection("test.db"));
        return new SQLiteSnapshotChangeEventSource(connectorConfig, snapshotterService, connectionFactory, null, null, null, null, null);
    }

    @Test
    void snapshotSelectListsTheColumnsAndQuotesTheTable() {
        Optional<String> select = newSource().getSnapshotSelect(null, new TableId(null, null, "products"), List.of("\"id\"", "\"name\""));

        assertThat(select).hasValue("SELECT \"id\", \"name\" FROM \"products\"");
    }

    @Test
    void snapshotSelectIsEmptyWhenNoColumns() {
        Optional<String> select = newSource().getSnapshotSelect(null, new TableId(null, null, "products"), List.of());

        assertThat(select).isEmpty();
    }
}
