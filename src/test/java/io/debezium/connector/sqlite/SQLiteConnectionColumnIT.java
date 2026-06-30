/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * Verifies that reading a live table through the inherited {@code readSchema} produces columns whose
 * JDBC type follows SQLite's affinity rules, not the type the driver reports, and that nullability
 * and the primary key come through correctly. This exercises the {@code overrideColumn} hook, which
 * needs a real database, so it is an integration test.
 */
public class SQLiteConnectionColumnIT {

    private Table readTable(SqliteTestHelper db, String tableName) throws Exception {
        try (SQLiteConnection connection = new SQLiteConnection(db.databaseFile().toString())) {
            Tables tables = new Tables();
            connection.readSchema(tables, null, null,
                    Tables.TableFilter.fromPredicate(id -> tableName.equals(id.table())), null, false);
            TableId id = tables.tableIds().stream()
                    .filter(t -> tableName.equals(t.table()))
                    .findFirst()
                    .orElseThrow();
            return tables.forTable(id);
        }
    }

    @Test
    void readsAffinityDerivedColumnTypesAndKey() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE affinities ("
                    + "  id INTEGER PRIMARY KEY,"
                    + "  c_int BIGINT NOT NULL,"
                    + "  c_text VARCHAR(20),"
                    + "  c_real DOUBLE,"
                    + "  c_num DECIMAL(10,2),"
                    + "  c_blob BLOB,"
                    + "  c_notype"
                    + ")");

            Table table = readTable(db, "affinities");

            assertThat(table.columnWithName("id").jdbcType()).isEqualTo(Types.BIGINT);
            assertThat(table.columnWithName("c_int").jdbcType()).isEqualTo(Types.BIGINT);
            assertThat(table.columnWithName("c_text").jdbcType()).isEqualTo(Types.VARCHAR);
            assertThat(table.columnWithName("c_real").jdbcType()).isEqualTo(Types.DOUBLE);
            assertThat(table.columnWithName("c_num").jdbcType()).isEqualTo(Types.NUMERIC);
            assertThat(table.columnWithName("c_blob").jdbcType()).isEqualTo(Types.VARBINARY);
            assertThat(table.columnWithName("c_notype").jdbcType()).isEqualTo(Types.VARBINARY);

            assertThat(table.columnWithName("c_int").isOptional()).isFalse();
            assertThat(table.columnWithName("c_text").isOptional()).isTrue();

            assertThat(table.primaryKeyColumnNames()).containsExactly("id");
        }
    }

    @Test
    void readsKeylessTableWithNoPrimaryKey() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE keyless (x INTEGER, y TEXT)");

            Table table = readTable(db, "keyless");

            assertThat(table.primaryKeyColumnNames()).isEmpty();
            assertThat(table.columnWithName("x").jdbcType()).isEqualTo(Types.BIGINT);
            assertThat(table.columnWithName("y").jdbcType()).isEqualTo(Types.VARCHAR);
        }
    }
}
