/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;

/**
 * Unit tests for the connector-specific {@code source} fields {@link SQLiteSourceInfo} renders: the
 * table name and the {@code change_id}.
 */
class SQLiteSourceInfoTest {

    private static SQLiteSourceInfo newSourceInfo() {
        Configuration config = Configuration.from(Map.of(
                SQLiteConnectorConfig.DATABASE_FILE.name(), "test.db",
                CommonConnectorConfig.TOPIC_PREFIX.name(), "test"));
        return new SQLiteSourceInfo(new SQLiteConnectorConfig(config));
    }

    @Test
    void structCarriesTheChangeTableTimeAndId() {
        SQLiteSourceInfo info = newSourceInfo();
        Instant committedAt = Instant.ofEpochMilli(1_700_000_000_000L);

        info.setChange(new TableId(null, null, "products"), committedAt, 42L);

        Struct source = info.struct();
        assertThat(source.getInt64(AbstractSourceInfo.TIMESTAMP_KEY)).isEqualTo(committedAt.toEpochMilli());
        assertThat(source.getString(AbstractSourceInfo.TABLE_NAME_KEY)).isEqualTo("products");
        assertThat(source.getInt64(SQLiteSourceInfo.CHANGE_ID_KEY)).isEqualTo(42L);
    }

    @Test
    void freshStructHasEmptyTableAndZeroChangeId() {
        Struct source = newSourceInfo().struct();

        assertThat(source.getString(AbstractSourceInfo.TABLE_NAME_KEY)).isEmpty();
        assertThat(source.getInt64(SQLiteSourceInfo.CHANGE_ID_KEY)).isEqualTo(0L);
    }
}
