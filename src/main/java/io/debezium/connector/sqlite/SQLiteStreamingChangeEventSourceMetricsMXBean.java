/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * SQLite-specific streaming metrics exposed over JMX in addition to the standard
 * {@link StreamingChangeEventSourceMetricsMXBean} attributes.
 */
public interface SQLiteStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * @return the {@code change_id} of the most recently dispatched CDC log row, or 0 before any row
     *         has been dispatched in the current streaming run
     */
    long getCurrentChangeId();

    /**
     * @return the {@code change_id} Kafka Connect has most recently committed, or 0 before any commit
     */
    long getCommittedChangeId();

    /**
     * @return the number of CDC log rows still present above the last committed {@code change_id}, the
     *         backlog not yet compacted; never negative
     */
    long getCdcLogDepth();
}
