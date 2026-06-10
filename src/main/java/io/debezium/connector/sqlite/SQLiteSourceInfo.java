/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Instant;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Carries the {@code source} metadata block included in every change event.
 *
 * <p>Add connector-specific fields here (e.g. the last processed {@code change_id}).
 * Any field added here must also be registered in {@link SQLiteSourceInfoStructMaker}.
 */
public class SQLiteSourceInfo extends BaseSourceInfo {

    public SQLiteSourceInfo(CommonConnectorConfig config) {
        super(config);
    }

    @Override
    protected Instant timestamp() {
        return Instant.now();
    }

    @Override
    protected String database() {
        return serverName();
    }
}
