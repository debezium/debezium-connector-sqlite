/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Full-boot integration test for the SQLite connector. With snapshot disabled and the streaming
 * source still a no-op, the connector must reach the running state against a real database file and
 * emit no records, then stop cleanly.
 */
public class SQLiteConnectorIT extends AbstractAsyncEngineConnectorTest {

    private SqliteTestHelper database;

    @BeforeEach
    public void prepareDatabase() throws Exception {
        database = SqliteTestHelper.create();
    }

    @AfterEach
    public void closeDatabase() throws Exception {
        // Stop the connector before removing the database file, then close the helper. The base
        // class also stops the connector after each test, which is a no-op once it is stopped.
        stopConnector();
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void shouldReachRunningStateAndEmitNoRecords() throws InterruptedException {
        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, "sqlite_boot")
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        // The pipeline is fully wired, but no change source emits anything in this phase.
        waitForAvailableRecords(1, TimeUnit.SECONDS);
        assertNoRecordsToConsume();

        stopConnector();
        assertConnectorNotRunning();
    }
}
