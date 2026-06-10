/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for the SQLite connector.
 */
public class SQLiteErrorHandler extends ErrorHandler {

    public SQLiteErrorHandler(CommonConnectorConfig connectorConfig,
                              ChangeEventQueue<?> queue,
                              ErrorHandler replacedErrorHandler) {
        super(SQLiteSourceConnector.class, connectorConfig, queue, replacedErrorHandler);
    }
}
