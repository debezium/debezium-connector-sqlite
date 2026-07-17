/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

/**
 * One row read from the {@code _debezium_cdc_log} table by the streaming poll query.
 *
 * @param changeId the row's log sequence number, the streaming offset
 * @param tableName the source table the change came from
 * @param operation the operation code: {@code c}, {@code u}, or {@code d}
 * @param oldRowData the row JSON before the change, or null for an insert
 * @param newRowData the row JSON after the change, or null for a delete
 * @param committedAt the commit time in Unix epoch milliseconds
 */
public record CdcLogRow(long changeId, String tableName, String operation,
        String oldRowData, String newRowData, long committedAt) {
}
