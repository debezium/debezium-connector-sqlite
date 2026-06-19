/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static io.debezium.connector.sqlite.SQLiteTypeAffinity.BLOB;
import static io.debezium.connector.sqlite.SQLiteTypeAffinity.INTEGER;
import static io.debezium.connector.sqlite.SQLiteTypeAffinity.NUMERIC;
import static io.debezium.connector.sqlite.SQLiteTypeAffinity.REAL;
import static io.debezium.connector.sqlite.SQLiteTypeAffinity.TEXT;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

/**
 * Verifies the five ordered, case-insensitive substring rules SQLite uses to derive a column's type
 * affinity from its declared type, including the classic gotchas the rules produce.
 */
public class SQLiteTypeAffinityTest {

    @Test
    void resolvesIntegerAffinity() {
        assertThat(SQLiteTypeAffinity.of("INT")).isEqualTo(INTEGER);
        assertThat(SQLiteTypeAffinity.of("INTEGER")).isEqualTo(INTEGER);
        assertThat(SQLiteTypeAffinity.of("BIGINT")).isEqualTo(INTEGER);
        assertThat(SQLiteTypeAffinity.of("TINYINT")).isEqualTo(INTEGER);
        assertThat(SQLiteTypeAffinity.of("INT8")).isEqualTo(INTEGER);
    }

    @Test
    void resolvesTextAffinity() {
        assertThat(SQLiteTypeAffinity.of("CHARACTER(20)")).isEqualTo(TEXT);
        assertThat(SQLiteTypeAffinity.of("VARCHAR(255)")).isEqualTo(TEXT);
        assertThat(SQLiteTypeAffinity.of("NVARCHAR")).isEqualTo(TEXT);
        assertThat(SQLiteTypeAffinity.of("CLOB")).isEqualTo(TEXT);
        assertThat(SQLiteTypeAffinity.of("TEXT")).isEqualTo(TEXT);
    }

    @Test
    void resolvesBlobAffinity() {
        assertThat(SQLiteTypeAffinity.of("BLOB")).isEqualTo(BLOB);
    }

    @Test
    void resolvesRealAffinity() {
        assertThat(SQLiteTypeAffinity.of("REAL")).isEqualTo(REAL);
        assertThat(SQLiteTypeAffinity.of("DOUBLE")).isEqualTo(REAL);
        assertThat(SQLiteTypeAffinity.of("DOUBLE PRECISION")).isEqualTo(REAL);
        assertThat(SQLiteTypeAffinity.of("FLOAT")).isEqualTo(REAL);
    }

    @Test
    void resolvesNumericAffinity() {
        assertThat(SQLiteTypeAffinity.of("NUMERIC")).isEqualTo(NUMERIC);
        assertThat(SQLiteTypeAffinity.of("DECIMAL(10,5)")).isEqualTo(NUMERIC);
        assertThat(SQLiteTypeAffinity.of("BOOLEAN")).isEqualTo(NUMERIC);
        assertThat(SQLiteTypeAffinity.of("DATE")).isEqualTo(NUMERIC);
        assertThat(SQLiteTypeAffinity.of("DATETIME")).isEqualTo(NUMERIC);
    }

    @Test
    void resolvesBlobForNoDeclaredType() {
        assertThat(SQLiteTypeAffinity.of(null)).isEqualTo(BLOB);
        assertThat(SQLiteTypeAffinity.of("")).isEqualTo(BLOB);
        assertThat(SQLiteTypeAffinity.of("   ")).isEqualTo(BLOB);
    }

    @Test
    void matchesOnSubstringNotEquality() {
        // POINT contains INT, so it takes INTEGER affinity despite being a spatial type name.
        assertThat(SQLiteTypeAffinity.of("POINT")).isEqualTo(INTEGER);
    }

    @Test
    void isCaseInsensitive() {
        assertThat(SQLiteTypeAffinity.of("int")).isEqualTo(INTEGER);
        assertThat(SQLiteTypeAffinity.of("Integer")).isEqualTo(INTEGER);
        assertThat(SQLiteTypeAffinity.of("tExT")).isEqualTo(TEXT);
        assertThat(SQLiteTypeAffinity.of("Double")).isEqualTo(REAL);
    }

    @Test
    void mapsEachAffinityToItsJdbcType() {
        assertThat(INTEGER.jdbcType()).isEqualTo(Types.BIGINT);
        assertThat(REAL.jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(TEXT.jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(BLOB.jdbcType()).isEqualTo(Types.VARBINARY);
        assertThat(NUMERIC.jdbcType()).isEqualTo(Types.NUMERIC);
    }
}
