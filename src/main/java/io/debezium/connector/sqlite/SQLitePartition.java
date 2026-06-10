/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Map;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;

/**
 * Identifies the offset partition for a SQLite connector instance.
 *
 * <p>Each connector instance writes its offsets under this partition key in Kafka Connect's
 * offset storage. Instances with different {@link SQLiteConnectorConfig#TOPIC_PREFIX} values
 * keep independent offsets.
 */
public class SQLitePartition extends AbstractPartition {

    private static final String SERVER_KEY = "server";

    private final String serverName;

    public SQLitePartition(String serverName) {
        super(serverName);
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Map.of(SERVER_KEY, serverName);
    }

    /**
     * Provides the set of partitions for a given connector configuration.
     */
    public static class Provider implements Partition.Provider<SQLitePartition> {

        private final SQLiteConnectorConfig config;

        public Provider(SQLiteConnectorConfig config) {
            this.config = config;
        }

        @Override
        public Set<SQLitePartition> getPartitions() {
            return Set.of(new SQLitePartition(config.getLogicalName()));
        }
    }
}
