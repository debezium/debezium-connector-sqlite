/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.debezium.util.HexConverter;

/**
 * Converts a {@code _debezium_cdc_log} row into a Debezium change record. The snapshot and streaming
 * sources create one per row and pass it to the {@link io.debezium.pipeline.EventDispatcher}, which
 * builds the key, value, and envelope structs from the table schema and the raw column data.
 */
class SQLiteChangeRecordEmitter extends RelationalChangeRecordEmitter<SQLitePartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteChangeRecordEmitter.class);

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
        // Warn from the old side only for a delete, where it is the row's one present side.
        return decode(oldRowData, newRowData == null);
    }

    @Override
    protected Object[] getNewColumnValues() {
        return decode(newRowData, newRowData != null);
    }

    /**
     * Decodes one side of the change into an array in {@link Table#columns()} order. A null string is
     * the absent side of an insert or delete and decodes to an empty array.
     *
     * <p>A column the current schema has but the captured JSON does not carry means the capture trigger
     * was stale when the row was written, from an {@code ALTER TABLE} that raced the trigger rebuild. The
     * value was never captured, so it is emitted as null and, when this is the row's present side, a
     * warning names the columns so the loss is visible.
     */
    private Object[] decode(String rowData, boolean warnOnStaleCapture) {
        if (rowData == null) {
            return new Object[0];
        }
        Document document = parse(rowData);
        List<Column> columns = table.columns();
        Object[] values = new Object[columns.size()];
        List<String> missing = null;
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            if (warnOnStaleCapture && !document.has(name)) {
                if (missing == null) {
                    missing = new ArrayList<>();
                }
                missing.add(name);
            }
            values[i] = columnValue(document.get(name));
        }
        if (missing != null) {
            LOGGER.warn("Change event for table '{}' is missing column(s) {} that the current schema expects; "
                    + "the capture trigger was stale when the row was written, so they are null", table.id(), missing);
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
     * Java null. A blob is written as a tagged object {@code {"__dbz_hex__": "<hex>"}}, so a value that
     * carries the marker is hex-decoded back to its byte array; every other value is read bare.
     */
    private static Object columnValue(Value value) {
        if (Value.isNull(value)) {
            return null;
        }
        if (value.isDocument()) {
            String hex = value.asDocument().getString(CdcLog.BLOB_HEX_MARKER);
            if (hex != null) {
                return HexConverter.convertFromHex(hex);
            }
        }
        return value.asObject();
    }
}
