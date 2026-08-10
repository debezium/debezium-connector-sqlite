/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.Types;
import java.util.Locale;

import io.debezium.util.Strings;

/**
 * The five type affinities SQLite assigns to a column, each mapped to the JDBC type a column of that
 * affinity is given.
 *
 * <p>SQLite stores a column's declared type verbatim and derives its affinity with case-insensitive
 * substring rules, implemented by {@link #of(String)}. See SQLite datatypes, section 3.1, at
 * https://www.sqlite.org/datatype3.html.
 */
enum SQLiteTypeAffinity {

    INTEGER(Types.BIGINT),
    TEXT(Types.VARCHAR),
    BLOB(Types.VARBINARY),
    REAL(Types.DOUBLE),
    NUMERIC(Types.NUMERIC);

    private final int jdbcType;

    SQLiteTypeAffinity(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    /**
     * The JDBC type a column of this affinity is given, used to correct the driver-reported type so
     * a column's JDBC type stays consistent with its affinity.
     *
     * @return the {@link Types} constant for this affinity
     */
    int jdbcType() {
        return jdbcType;
    }

    /**
     * Resolves the affinity of a column from its declared type string, applying SQLite's five
     * ordered rules. A null or blank declared type resolves to {@link #BLOB}, matching SQLite's
     * treatment of a column declared with no type.
     *
     * @param declaredType the column's declared type as written in the schema, may be null or blank
     * @return the affinity SQLite assigns to a column of that declared type
     */
    static SQLiteTypeAffinity of(String declaredType) {
        if (Strings.isNullOrBlank(declaredType)) {
            return BLOB;
        }
        String type = declaredType.toUpperCase(Locale.ENGLISH);
        if (type.contains("INT")) {
            return INTEGER;
        }
        if (type.contains("CHAR") || type.contains("CLOB") || type.contains("TEXT")) {
            return TEXT;
        }
        if (type.contains("BLOB")) {
            return BLOB;
        }
        if (type.contains("REAL") || type.contains("FLOA") || type.contains("DOUB")) {
            return REAL;
        }
        return NUMERIC;
    }
}
