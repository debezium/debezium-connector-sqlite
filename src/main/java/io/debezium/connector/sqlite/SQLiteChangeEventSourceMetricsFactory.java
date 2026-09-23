/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Returns the SQLite streaming metrics so the connector's {@code change_id} position and CDC log depth
 * are exposed over JMX; snapshot metrics stay the framework default. The streaming metrics instance is
 * created in the task and shared, so the coordinator registers the same object the streaming source
 * updates.
 */
public class SQLiteChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<SQLitePartition> {

    private final SQLiteStreamingChangeEventSourceMetrics streamingMetrics;

    public SQLiteChangeEventSourceMetricsFactory(SQLiteStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<SQLitePartition> getStreamingMetrics(
                                                                                                                   T taskContext,
                                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                   EventMetadataProvider eventMetadataProvider,
                                                                                                                   CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }
}
