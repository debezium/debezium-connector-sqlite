/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link SQLiteChangeRecordEmitter}'s decode of the {@code _debezium_cdc_log} row JSON
 * into column-ordered value arrays and its mapping of the operation code.
 */
class SQLiteChangeRecordEmitterTest {

    private static SQLiteConnectorConfig config() {
        return new SQLiteConnectorConfig(Configuration.from(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "test.db",
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test")));
    }

    private static Table table(String... columnNames) {
        var editor = Table.editor().tableId(new TableId(null, null, "t"));
        for (String name : columnNames) {
            editor.addColumn(Column.editor().name(name).create());
        }
        return editor.create();
    }

    private static SQLiteChangeRecordEmitter emitter(Envelope.Operation operation, Table table,
                                                     String oldRowData, String newRowData) {
        return new SQLiteChangeRecordEmitter(null, null, operation, table, oldRowData, newRowData, null, config());
    }

    @Test
    void decodesColumnsInTableOrderFillingAMissingColumnWithNull() {
        Table table = table("id", "name", "score");
        Object[] values = emitter(Envelope.Operation.CREATE, table, null, "{\"id\":1,\"name\":\"alice\"}")
                .getNewColumnValues();

        assertThat(values).hasSize(3);
        assertThat(((Number) values[0]).intValue()).isEqualTo(1);
        assertThat(values[1]).isEqualTo("alice");
        assertThat(values[2]).isNull();
    }

    @Test
    void decodesAnExplicitJsonNullToNull() {
        Table table = table("id", "note");
        Object[] values = emitter(Envelope.Operation.CREATE, table, null, "{\"id\":1,\"note\":null}")
                .getNewColumnValues();

        assertThat(values[1]).isNull();
    }

    @Test
    void insertHasNoOldValues() {
        Table table = table("id");
        SQLiteChangeRecordEmitter emitter = emitter(Envelope.Operation.CREATE, table, null, "{\"id\":1}");

        assertThat(emitter.getOldColumnValues()).isEmpty();
        assertThat(emitter.getNewColumnValues()).hasSize(1);
    }

    @Test
    void deleteHasNoNewValues() {
        Table table = table("id");
        SQLiteChangeRecordEmitter emitter = emitter(Envelope.Operation.DELETE, table, "{\"id\":1}", null);

        assertThat(emitter.getNewColumnValues()).isEmpty();
        assertThat(emitter.getOldColumnValues()).hasSize(1);
    }

    @Test
    void mapsOperationCodesToChangeOperations() {
        assertThat(SQLiteChangeRecordEmitter.operationFor(CdcLog.OPERATION_CREATE)).isEqualTo(Envelope.Operation.CREATE);
        assertThat(SQLiteChangeRecordEmitter.operationFor(CdcLog.OPERATION_UPDATE)).isEqualTo(Envelope.Operation.UPDATE);
        assertThat(SQLiteChangeRecordEmitter.operationFor(CdcLog.OPERATION_DELETE)).isEqualTo(Envelope.Operation.DELETE);
    }

    @Test
    void rejectsAnUnknownOperationCode() {
        assertThatThrownBy(() -> SQLiteChangeRecordEmitter.operationFor("x"))
                .isInstanceOf(DebeziumException.class);
    }
}
