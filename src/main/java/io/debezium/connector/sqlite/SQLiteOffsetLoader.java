/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Map;

import io.debezium.pipeline.spi.OffsetContext;

/**
 * Restores a {@link SQLiteOffsetContext} from Kafka Connect's persisted offset storage.
 *
 * <p>Called once on connector start. When {@code offset} is null or empty the connector has never
 * run before and a full snapshot should be performed. Otherwise, streaming resumes from the stored
 * {@code change_id}.
 */
public class SQLiteOffsetLoader implements OffsetContext.Loader<SQLiteOffsetContext> {

    private final SQLiteConnectorConfig config;

    public SQLiteOffsetLoader(SQLiteConnectorConfig config) {
        this.config = config;
    }

    @Override
    public SQLiteOffsetContext load(Map<String, ?> offset) {
        SQLiteSourceInfo sourceInfo = new SQLiteSourceInfo(config);
        SQLiteOffsetContext ctx = new SQLiteOffsetContext(sourceInfo);

        if (offset == null || offset.isEmpty()) {
            ctx.setChangeId(0L);
        }
        else {
            long changeId = ((Number) offset.get(SQLiteOffsetContext.CHANGE_ID_KEY)).longValue();
            ctx.setChangeId(changeId);
        }
        return ctx;
    }
}
