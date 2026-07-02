/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
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

    private Object convert(String declaredType, Object data) {
        Column column = Column.editor().name("c").type(declaredType).create();
        return converter.converter(column, null).convert(data);
    }

    @Test
    void adaptsValuesToEachAffinitysType() {
        assertThat(convert("BIGINT", 42)).isEqualTo(42L);
        assertThat(convert("DOUBLE", 1)).isEqualTo(1.0d);
        assertThat(convert("DECIMAL", 2)).isEqualTo(2.0d);
        assertThat(convert("VARCHAR", 7)).isEqualTo("7");
        assertThat(convert("BLOB", new byte[]{ 1, 2 })).isEqualTo(new byte[]{ 1, 2 });
    }

    @Test
    void passesNullThrough() {
        assertThat(convert("BIGINT", null)).isNull();
        assertThat(convert("DOUBLE", null)).isNull();
        assertThat(convert("VARCHAR", null)).isNull();
        assertThat(convert("BLOB", null)).isNull();
    }

    @Test
    void throwsWhenStorageClassCannotBeRepresented() {
        // A blob or non-numeric text physically stored in a numeric column.
        assertThatThrownBy(() -> convert("BIGINT", "hello")).isInstanceOf(DebeziumException.class);
        assertThatThrownBy(() -> convert("DOUBLE", "hello")).isInstanceOf(DebeziumException.class);
        assertThatThrownBy(() -> convert("BIGINT", new byte[]{ 1 })).isInstanceOf(DebeziumException.class);
        // A blob in a text column, which would otherwise render as a garbage String.
        assertThatThrownBy(() -> convert("VARCHAR", new byte[]{ 1 })).isInstanceOf(DebeziumException.class);
        // A number or text in a blob column.
        assertThatThrownBy(() -> convert("BLOB", 5)).isInstanceOf(DebeziumException.class);
        assertThatThrownBy(() -> convert("BLOB", "x")).isInstanceOf(DebeziumException.class);
    }
}
