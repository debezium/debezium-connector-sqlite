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
 * Converts a row from the {@code _debezium_cdc_log} table into a Debezium change record
 * and emits it to the framework.
 *
 * <p>The snapshot and streaming sources create one instance per CDC log row and pass it to
 * {@link io.debezium.pipeline.EventDispatcher#dispatchDataChangeEvent}. The dispatcher calls
 * {@link #emitChangeRecords}, which uses the table schema together with the raw column data
 * to build the key, value, and envelope structs before forwarding them to the receiver.
 *
 * <p>{@link #getOldColumnValues()} and {@link #getNewColumnValues()} will decode the JSON
 * {@code old_row_data} and {@code new_row_data} from the CDC log row in Phase 1.
 */
class SQLiteChangeRecordEmitter extends RelationalChangeRecordEmitter<SQLitePartition> {

    private final Envelope.Operation operation;

    /**
     * Raw data for this CDC event decoded from the {@code _debezium_cdc_log} row.
     * Typed column arrays are derived from this in {@link #getOldColumnValues()} and
     * {@link #getNewColumnValues()}.
     */
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
        // TODO: decode old_row_data JSON into typed column value array in Phase 1.
        return new Object[0];
    }

    @Override
    protected Object[] getNewColumnValues() {
        // TODO: decode new_row_data JSON into typed column value array in Phase 1.
        return new Object[0];
    }
}
