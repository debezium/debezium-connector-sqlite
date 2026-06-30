/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * Verifies that {@link TriggerGenerator} builds well-formed trigger SQL that names the right columns
 * and operation codes. The generated statements are created against an in-memory SQLite database, so
 * SQLite itself confirms they parse, and the test stays a Surefire unit test with no temp file. The
 * end-to-end check that the triggers actually populate {@code _debezium_cdc_log} lives in the helper
 * integration test.
 */
public class TriggerGeneratorTest {

    private static final String TABLE = "users";
    private static final List<String> COLUMNS = List.of("id", "name");

    @Test
    void buildsThreeTriggersThatSqliteAccepts() throws Exception {
        try (JdbcConnection connection = memoryConnection()) {
            connection.execute(
                    CdcLog.CREATE_TABLE_DDL,
                    "CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, name TEXT)");
            connection.execute(TriggerGenerator.createTriggers(TABLE, COLUMNS).toArray(new String[0]));

            assertThat(triggerNames(connection)).containsExactlyInAnyOrder(
                    "_debezium_cdc_users_insert",
                    "_debezium_cdc_users_update",
                    "_debezium_cdc_users_delete");
        }
    }

    @Test
    void generatesCorrectOperationCodesAndColumns() {
        List<String> triggers = TriggerGenerator.createTriggers(TABLE, COLUMNS);
        String insert = triggers.get(0);
        String update = triggers.get(1);
        String delete = triggers.get(2);

        // An insert records the new row only, under operation code 'c'.
        assertThat(insert).contains("AFTER INSERT")
                .contains("'" + CdcLog.OPERATION_CREATE + "'")
                .contains("json_object('id', NEW.\"id\", 'name', NEW.\"name\")")
                .contains("NULL,");

        // An update records both old and new rows, under operation code 'u'.
        assertThat(update).contains("AFTER UPDATE")
                .contains("'" + CdcLog.OPERATION_UPDATE + "'")
                .contains("json_object('id', OLD.\"id\", 'name', OLD.\"name\")")
                .contains("json_object('id', NEW.\"id\", 'name', NEW.\"name\")");

        // A delete records the old row only, under operation code 'd'.
        assertThat(delete).contains("AFTER DELETE")
                .contains("'" + CdcLog.OPERATION_DELETE + "'")
                .contains("json_object('id', OLD.\"id\", 'name', OLD.\"name\")");
    }

    /** Names of every trigger SQLite has stored, read from {@code sqlite_master}. */
    private static List<String> triggerNames(JdbcConnection connection) throws Exception {
        return connection.queryAndMap("SELECT name FROM sqlite_master WHERE type='trigger'", rs -> {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString("name"));
            }
            return names;
        });
    }

    /** A connection to a fresh in-memory SQLite database, reused for the life of the connection. */
    private static JdbcConnection memoryConnection() {
        return new JdbcConnection(
                JdbcConfiguration.empty(),
                config -> DriverManager.getConnection("jdbc:sqlite::memory:"),
                "\"", "\"");
    }
}
