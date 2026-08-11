/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this module.
 */
public class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/sqlite/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /** The symbolic name of the connector plugin. */
    public static String name() {
        return "sqlite";
    }

    /** The context name used in log MDC and JMX metrics. */
    public static String contextName() {
        return "SQLite";
    }
}
