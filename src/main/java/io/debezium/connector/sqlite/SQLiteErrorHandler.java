/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import org.sqlite.SQLiteException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for the SQLite connector.
 */
public class SQLiteErrorHandler extends ErrorHandler {

    /** Masks an extended result code (e.g. {@code SQLITE_BUSY_SNAPSHOT}) down to its primary code. */
    private static final int PRIMARY_RESULT_CODE_MASK = 0xFF;

    /** SQLite's primary result code for a database locked by another connection's write. */
    private static final int SQLITE_BUSY = 5;

    /** SQLite's primary result code for a table locked by another connection within the same process. */
    private static final int SQLITE_LOCKED = 6;

    public SQLiteErrorHandler(CommonConnectorConfig connectorConfig,
                              ChangeEventQueue<?> queue,
                              ErrorHandler replacedErrorHandler) {
        super(SQLiteSourceConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    /**
     * Retries only transient SQLite contention, {@code SQLITE_BUSY} and {@code SQLITE_LOCKED}
     * (including their extended variants). Every other SQLite error, such as a constraint violation or a
     * corrupted database, is fatal and a retry will not fix it.
     */
    @Override
    protected boolean isRetriable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLiteException) {
                int primaryCode = ((SQLiteException) current).getResultCode().code & PRIMARY_RESULT_CODE_MASK;
                if (primaryCode == SQLITE_BUSY || primaryCode == SQLITE_LOCKED) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return super.isRetriable(throwable);
    }
}
