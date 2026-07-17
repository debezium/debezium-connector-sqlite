/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.io.IOException;
import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.Value;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Converts a {@code _debezium_cdc_log} row into a Debezium change record. The snapshot and streaming
 * sources create one per row and pass it to the {@link io.debezium.pipeline.EventDispatcher}, which
 * builds the key, value, and envelope structs from the table schema and the raw column data.
 */
class SQLiteChangeRecordEmitter extends RelationalChangeRecordEmitter<SQLitePartition> {

    private final Envelope.Operation operation;
    private final Table table;
    private final String oldRowData;
    private final String newRowData;

    SQLiteChangeRecordEmitter(SQLitePartition partition,
                              OffsetContext offsetContext,
                              Envelope.Operation operation,
                              Table table,
                              String oldRowData,
                              String newRowData,
                              Clock clock,
                              SQLiteConnectorConfig config) {
        super(partition, offsetContext, clock, config);
        this.operation = operation;
        this.table = table;
        this.oldRowData = oldRowData;
        this.newRowData = newRowData;
    }

    /** Maps a {@code _debezium_cdc_log} operation code to the change operation the framework expects. */
    static Envelope.Operation operationFor(String operationCode) {
        switch (operationCode) {
            case CdcLog.OPERATION_CREATE:
                return Envelope.Operation.CREATE;
            case CdcLog.OPERATION_UPDATE:
                return Envelope.Operation.UPDATE;
            case CdcLog.OPERATION_DELETE:
                return Envelope.Operation.DELETE;
            default:
                throw new DebeziumException("Unknown " + CdcLog.TABLE_NAME + " operation code: " + operationCode);
        }
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return decode(oldRowData);
    }

    @Override
    protected Object[] getNewColumnValues() {
        return decode(newRowData);
    }

    /**
     * Decodes one side of the change into an array in {@link Table#columns()} order. A null string is
     * the absent side of an insert or delete and decodes to an empty array.
     */
    private Object[] decode(String rowData) {
        if (rowData == null) {
            return new Object[0];
        }
        Document document = parse(rowData);
        List<Column> columns = table.columns();
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            values[i] = columnValue(document.get(columns.get(i).name()));
        }
        return values;
    }

    private Document parse(String rowData) {
        try {
            return DocumentReader.defaultReader().read(rowData);
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to parse " + CdcLog.TABLE_NAME + " row JSON: " + rowData, e);
        }
    }

    /**
     * Turns a decoded JSON value into the column value. An absent column and a JSON null both yield
     * Java null.
     */
    private static Object columnValue(Value value) {
        return Value.isNull(value) ? null : value.asObject();
    }
}
