/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

/**
 * One row read from {@code _debezium_cdc_log} by the streaming poll query. {@code changeId} is the log
 * sequence number and the streaming offset; {@code oldRowData} is null for an insert and
 * {@code newRowData} null for a delete; {@code committedAt} is Unix epoch milliseconds.
 */
public record CdcLogRow(long changeId, String tableName, String operation,
        String oldRowData, String newRowData, long committedAt) {
}
