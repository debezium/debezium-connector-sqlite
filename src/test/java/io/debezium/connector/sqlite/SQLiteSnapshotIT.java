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
 * Integration test for the initial snapshot. Booting with {@code snapshot.mode=initial} must emit one
 * read ({@code r}) record per existing row of every monitored table, with the row's values in the
 * {@code after} struct typed by SQLite affinity, and {@code snapshot: last} on the single final record.
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

    @Test
    public void shouldSnapshotEveryTableWithTypedValuesAndMarkOnlyTheLastRecord() throws Exception {
        database.connection().execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
                "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, qty BIGINT, data BLOB)");
        database.connection().execute(
                "INSERT INTO orders (id, total) VALUES (10, 99.5)",
                "INSERT INTO products (id, name, price, qty, data) VALUES (1, 'Widget', 2.5, 100, x'0102')",
                "INSERT INTO products (id, name, price, qty, data) VALUES (2, 'Gadget', 9.0, 7, x'03')");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "initial")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(3, false);

        // Every monitored table is snapshotted, each row as a read record.
        List<SourceRecord> orders = records.recordsForTopic(TOPIC_PREFIX + ".orders");
        List<SourceRecord> products = records.recordsForTopic(TOPIC_PREFIX + ".products");
        assertThat(orders).hasSize(1);
        assertThat(products).hasSize(2);
        assertThat(records.allRecordsInOrder()).allSatisfy(record -> assertThat(
                ((Struct) record.value()).getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.READ.code()));

        // Keys are the integer primary keys.
        assertThat(orders).extracting(record -> ((Struct) record.key()).getInt64("id")).containsExactly(10L);
        assertThat(products).extracting(record -> ((Struct) record.key()).getInt64("id")).containsExactlyInAnyOrder(1L, 2L);

        // Value fields carry the SQLite affinities: REAL as a double, BIGINT as a long, BLOB as bytes.
        Struct widget = products.stream().map(SQLiteSnapshotIT::after).filter(after -> after.getInt64("id") == 1L).findFirst().orElseThrow();
        assertThat(widget.getString("name")).isEqualTo("Widget");
        assertThat(widget.getFloat64("price")).isEqualTo(2.5);
        assertThat(widget.getInt64("qty")).isEqualTo(100L);
        assertThat(widget.getBytes("data")).isEqualTo(new byte[]{ 1, 2 });
        assertThat(after(orders.get(0)).getFloat64("total")).isEqualTo(99.5);

        // Only the very last record of the whole snapshot is marked last, and it is a row of the last
        // table read.
        List<SourceRecord> all = records.allRecordsInOrder();
        SourceRecord last = all.get(all.size() - 1);
        assertThat(last.topic()).isEqualTo(TOPIC_PREFIX + ".products");
        assertThat(((Struct) last.value()).getStruct(Envelope.FieldName.SOURCE).getString("snapshot")).isEqualTo("last");
        long markedLast = all.stream()
                .map(record -> ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).getString("snapshot"))
                .filter("last"::equals).count();
        assertThat(markedLast).isEqualTo(1L);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }
}
