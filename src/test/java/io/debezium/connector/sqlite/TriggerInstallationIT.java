/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Integration test for trigger installation at startup. The connector installs the capture triggers
 * itself when it starts, so a plain SQL insert, update, and delete on a monitored table each land the
 * matching {@code c}, {@code u}, and {@code d} row in {@code _debezium_cdc_log}, in ascending
 * {@code change_id} order. No trigger is installed by the test; the connector does it.
 */
public class TriggerInstallationIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC_PREFIX = "sqlite_trig";

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
    public void shouldInstallTriggersThatCaptureInsertUpdateDelete() throws Exception {
        database.connection().execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");

        Configuration config = Configuration.create()
                .with(SQLiteConnectorConfig.DATABASE_FILE, database.databaseFile().toString())
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(SQLiteConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build();

        start(SQLiteSourceConnector.class, config);
        assertConnectorIsRunning();

        // The connector installed the triggers during startup, so these writes are captured without the
        // test installing anything itself.
        JdbcConnection db = database.connection();
        db.execute("INSERT INTO customers (id, name) VALUES (1, 'Alice')");
        db.execute("UPDATE customers SET name = 'Alicia' WHERE id = 1");
        db.execute("DELETE FROM customers WHERE id = 1");

        List<CdcRow> changes = readCdcLog(db);
        assertThat(changes).hasSize(3);

        CdcRow insert = changes.get(0);
        assertThat(insert.operation).isEqualTo(CdcLog.OPERATION_CREATE);
        assertThat(insert.table).isEqualTo("customers");
        assertThat(insert.oldRow).isNull();
        assertThat(insert.newRow).contains("\"id\":1", "\"name\":\"Alice\"");

        CdcRow update = changes.get(1);
        assertThat(update.operation).isEqualTo(CdcLog.OPERATION_UPDATE);
        assertThat(update.table).isEqualTo("customers");
        assertThat(update.oldRow).contains("\"name\":\"Alice\"");
        assertThat(update.newRow).contains("\"name\":\"Alicia\"");

        CdcRow delete = changes.get(2);
        assertThat(delete.operation).isEqualTo(CdcLog.OPERATION_DELETE);
        assertThat(delete.table).isEqualTo("customers");
        assertThat(delete.oldRow).contains("\"name\":\"Alicia\"");
        assertThat(delete.newRow).isNull();
    }

    /** One row of {@code _debezium_cdc_log}, in change order. */
    private record CdcRow(String table, String operation, String oldRow, String newRow) {
    }

    /** Reads every CDC log row, ordered by {@code change_id}. */
    private static List<CdcRow> readCdcLog(JdbcConnection db) throws SQLException {
        String query = "SELECT " + CdcLog.TABLE_NAME_COLUMN + ", " + CdcLog.OPERATION + ", "
                + CdcLog.OLD_ROW_DATA + ", " + CdcLog.NEW_ROW_DATA
                + " FROM " + CdcLog.TABLE_NAME + " ORDER BY " + CdcLog.CHANGE_ID;
        return db.queryAndMap(query, rs -> {
            List<CdcRow> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(new CdcRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rows;
        });
    }
}
