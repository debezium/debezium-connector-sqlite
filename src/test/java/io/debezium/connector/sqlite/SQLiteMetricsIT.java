/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration test that the SQLite-specific streaming metrics are registered and readable over JMX
 * while the connector runs, reporting the current {@code change_id} and the CDC log depth alongside the
 * standard streaming metrics.
 */
public class SQLiteMetricsIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_metrics";

    private SqliteTestHelper database;

    @BeforeEach
    public void prepareDatabase() throws Exception {
        database = SqliteTestHelper.create();
    }

    @AfterEach
    public void closeDatabase() throws Exception {
        stopConnector();
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void shouldExposeSqliteStreamingMetricsOverJmx() throws Exception {
        LogInterceptor streamingLog = new LogInterceptor(SQLiteStreamingChangeEventSource.class);

        database.connection().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER)");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> streamingLog.containsMessage("Starting SQLite streaming from change_id 0"));

        for (int i = 1; i <= 3; i++) {
            database.connection().execute("INSERT INTO t (id, seq) VALUES (" + i + ", " + i + ")");
        }

        consumeRecordsByTopic(3, false);

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName streaming = streamingMetricsName(mbeanServer);

        // The current change_id and the CDC log depth both catch up to the three uncompacted writes,
        // proving the SQLite-specific attributes are live and updated from the poll loop.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(readLong(mbeanServer, streaming, "CurrentChangeId")).isEqualTo(3L);
            assertThat(readLong(mbeanServer, streaming, "CdcLogDepth")).isEqualTo(3L);
        });

        // A standard streaming attribute is exposed on the same MBean.
        assertThat(readLong(mbeanServer, streaming, "TotalNumberOfEventsSeen")).isGreaterThanOrEqualTo(3L);
    }

    private static ObjectName streamingMetricsName(MBeanServer mbeanServer) throws Exception {
        ObjectName pattern = new ObjectName("*:type=connector-metrics,context=streaming,server=" + TOPIC_PREFIX + ",*");
        Set<ObjectName> names = mbeanServer.queryNames(pattern, null);
        assertThat(names).hasSize(1);
        return names.iterator().next();
    }

    private static long readLong(MBeanServer mbeanServer, ObjectName name, String attribute) throws Exception {
        return ((Number) mbeanServer.getAttribute(name, attribute)).longValue();
    }
}
