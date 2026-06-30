/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.sqlite.SQLiteSourceConnector;
import io.debezium.snapshot.spi.SnapshotQuery;

/**
 * Builds the {@code SELECT} statement that reads all rows of a table during a snapshot.
 *
 * <p>The snapshotter service resolves a {@link SnapshotQuery} by the {@code snapshot.query.mode}
 * configuration, which defaults to {@code select_all}. The connector must register one for the
 * service to be constructed, so this class provides the {@code select_all} query the service uses.
 */
@ConnectorSpecific(connector = SQLiteSourceConnector.class)
public class SelectAllSnapshotQuery implements SnapshotQuery {

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {
        if (snapshotSelectColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(snapshotSelectColumns.stream()
                .collect(Collectors.joining(", ", "SELECT ", " FROM " + tableId)));
    }
}
