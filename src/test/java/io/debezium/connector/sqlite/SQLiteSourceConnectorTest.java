/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SQLiteSourceConnectorTest {

    @Test
    void taskConfigsRejectsMoreThanOneTask() {
        SQLiteSourceConnector connector = new SQLiteSourceConnector();
        connector.start(Map.of());
        assertThatThrownBy(() -> connector.taskConfigs(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void taskConfigsReturnsTheSingleConfigForOneTask() {
        SQLiteSourceConnector connector = new SQLiteSourceConnector();
        Map<String, String> props = Map.of("database.file.path", "/tmp/example.db");
        connector.start(props);

        List<Map<String, String>> configs = connector.taskConfigs(1);

        assertThat(configs).hasSize(1);
        assertThat(configs.get(0)).isEqualTo(props);
    }
}
