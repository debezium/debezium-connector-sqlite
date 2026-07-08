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
                .contains("json_object('id', " + blobSafe("NEW", "id") + ", 'name', " + blobSafe("NEW", "name") + ")")
                .contains("NULL,");

        // An update records both old and new rows, under operation code 'u'.
        assertThat(update).contains("AFTER UPDATE")
                .contains("'" + CdcLog.OPERATION_UPDATE + "'")
                .contains("json_object('id', " + blobSafe("OLD", "id") + ", 'name', " + blobSafe("OLD", "name") + ")")
                .contains("json_object('id', " + blobSafe("NEW", "id") + ", 'name', " + blobSafe("NEW", "name") + ")");

        // A delete records the old row only, under operation code 'd'.
        assertThat(delete).contains("AFTER DELETE")
                .contains("'" + CdcLog.OPERATION_DELETE + "'")
                .contains("json_object('id', " + blobSafe("OLD", "id") + ", 'name', " + blobSafe("OLD", "name") + ")");
    }

    @Test
    void wrapsEachColumnValueInABlobSafeCase() {
        String insert = TriggerGenerator.createTriggers(TABLE, COLUMNS).get(0);

        // Every column value is wrapped in a runtime typeof check, so a blob in any column is
        // hex-encoded into the tagged object and every other value stays bare.
        assertThat(insert)
                .contains(blobSafe("NEW", "id"))
                .contains(blobSafe("NEW", "name"))
                .contains("json_object('" + CdcLog.BLOB_HEX_MARKER + "', hex(NEW.\"id\"))");
    }

    @Test
    void narrowTableDoesNotUseJsonSet() {
        // A small column list fits one json_object call, so no merge is needed.
        String insert = TriggerGenerator.createTriggers(TABLE, sequentialColumns(40)).get(0);
        assertThat(insert).doesNotContain("json_set(");
    }

    @Test
    void wideTableChunksWithJsonSetAndQuotesAwkwardNames() {
        List<String> columns = new ArrayList<>(sequentialColumns(60));
        // An awkward name, in a later chunk so it is serialized through the json_set path.
        columns.set(55, "w\"x");
        String insert = TriggerGenerator.createTriggers(TABLE, columns).get(0);

        // Past the chunk limit the row is merged with json_set rather than one json_object.
        assertThat(insert).contains("json_set(");
        // The name is escaped for the json path and the surrounding SQL literal, and its value
        // reference doubles the embedded quote.
        assertThat(insert).contains("'$.\"w\\\"x\"'")
                .contains("NEW.\"w\"\"x\"");
    }

    /** The blob-safe value expression the generator emits for one column, per row alias. */
    private static String blobSafe(String alias, String column) {
        String ref = alias + ".\"" + column + "\"";
        return "CASE WHEN typeof(" + ref + ")='blob' THEN json_object('" + CdcLog.BLOB_HEX_MARKER
                + "', hex(" + ref + ")) ELSE " + ref + " END";
    }

    /** A list of {@code count} column names {@code c0..c(count-1)}. */
    private static List<String> sequentialColumns(int count) {
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            columns.add("c" + i);
        }
        return columns;
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
