/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;

/**
 * Verifies that {@link SQLiteValueConverter#schemaBuilder(Column)} returns the Kafka Connect schema
 * type each SQLite affinity maps to, and leaves nullability to the framework.
 */
public class SQLiteValueConverterTest {

    private final SQLiteValueConverter converter = new SQLiteValueConverter();

    private Schema schemaFor(String declaredType) {
        Column column = Column.editor().name("c").type(declaredType).create();
        return converter.schemaBuilder(column).build();
    }

    @Test
    void mapsEachAffinityToItsConnectType() {
        assertThat(schemaFor("BIGINT").type()).isEqualTo(Schema.Type.INT64);
        assertThat(schemaFor("DOUBLE").type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(schemaFor("VARCHAR").type()).isEqualTo(Schema.Type.STRING);
        assertThat(schemaFor("BLOB").type()).isEqualTo(Schema.Type.BYTES);
        assertThat(schemaFor("DECIMAL").type()).isEqualTo(Schema.Type.FLOAT64);
    }

    @Test
    void mapsNoDeclaredTypeToBytes() {
        assertThat(schemaFor(null).type()).isEqualTo(Schema.Type.BYTES);
    }

    @Test
    void leavesNullabilityToTheFramework() {
        // schemaBuilder returns a required schema; TableSchemaBuilder applies optional() from the column.
        assertThat(schemaFor("VARCHAR").isOptional()).isFalse();
    }
}
