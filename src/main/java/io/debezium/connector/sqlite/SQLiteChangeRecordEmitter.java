/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Converts a {@code _debezium_cdc_log} row into a Debezium change record. The snapshot and streaming
 * sources create one per row and pass it to the {@link io.debezium.pipeline.EventDispatcher}, which
 * builds the key, value, and envelope structs from the table schema and the raw column data.
 */
class SQLiteChangeRecordEmitter extends RelationalChangeRecordEmitter<SQLitePartition> {

    private final Envelope.Operation operation;

    private final Object rowData;

    SQLiteChangeRecordEmitter(SQLitePartition partition,
                              OffsetContext offsetContext,
                              Envelope.Operation operation,
                              Object rowData,
                              Clock clock,
                              SQLiteConnectorConfig config) {
        super(partition, offsetContext, clock, config);
        this.operation = operation;
        this.rowData = rowData;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        // TODO: decode old_row_data JSON into a typed column value array.
        return new Object[0];
    }

    @Override
    protected Object[] getNewColumnValues() {
        // TODO: decode new_row_data JSON into a typed column value array.
        return new Object[0];
    }
}
