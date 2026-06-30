/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration test for the initial snapshot. Booting with {@code snapshot.mode=initial} against a
 * seeded table must emit one read ({@code r}) record per existing row, with the row's values in the
 * {@code after} struct and {@code snapshot: last} on the final record.
 */
public class SQLiteSnapshotIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_snap";

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
    public void shouldSnapshotExistingRowsAsReadRecords() throws Exception {
        database.connection().execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");
        database.connection().execute(
                "INSERT INTO customers (id, name) VALUES (1, 'Alice')",
                "INSERT INTO customers (id, name) VALUES (2, 'Bob')",
                "INSERT INTO customers (id, name) VALUES (3, 'Carol')");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "initial")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        // assertRecords=false skips the Avro/Apicurio schema validation, which the connector does not
        // pull in as a test dependency.
        SourceRecords records = consumeRecordsByTopic(3, false);
        List<SourceRecord> customers = records.recordsForTopic(TOPIC_PREFIX + ".customers");
        assertThat(customers).hasSize(3);

        for (SourceRecord record : customers) {
            Struct value = (Struct) record.value();
            assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.READ.code());
        }

        assertThat(customers).extracting(record -> ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER).getInt64("id"))
                .containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(customers).extracting(record -> ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER).getString("name"))
                .containsExactlyInAnyOrder("Alice", "Bob", "Carol");

        Struct lastSource = (Struct) ((Struct) customers.get(customers.size() - 1).value()).getStruct(Envelope.FieldName.SOURCE);
        assertThat(lastSource.getString("snapshot")).isEqualTo("last");
    }
}
