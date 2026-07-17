/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.time.Instant;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Carries the {@code source} metadata block included in every change event. Fields added here must
 * also be registered in {@link SQLiteSourceInfoStructMaker}.
 */
public class SQLiteSourceInfo extends BaseSourceInfo {

    static final String CHANGE_ID_KEY = "change_id";

    private Instant timestamp;
    private String tableName;
    private long changeId;

    public SQLiteSourceInfo(CommonConnectorConfig config) {
        super(config);
    }

    /** Records the change the next event describes. */
    void setChange(TableId tableId, Instant timestamp, long changeId) {
        this.tableName = tableId != null ? tableId.table() : null;
        this.timestamp = timestamp;
        this.changeId = changeId;
    }

    String tableName() {
        return tableName;
    }

    long changeId() {
        return changeId;
    }

    @Override
    protected Instant timestamp() {
        return timestamp != null ? timestamp : Instant.now();
    }

    @Override
    protected String database() {
        return serverName();
    }
}
