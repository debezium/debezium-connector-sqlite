/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TriggerReconciler#triggersMatch}, the decision that drives a rebuild. The
 * installed SQL is simulated the way SQLite stores it: the generated statements with {@code IF NOT
 * EXISTS} stripped.
 */
class TriggerReconcilerTest {

    private static final String TABLE = "orders";

    @Test
    void matchesWhenInstalledEqualsDesired() {
        List<String> columns = List.of("id", "name");
        List<String> desired = TriggerGenerator.createTriggers(TABLE, columns);

        assertThat(TriggerReconciler.triggersMatch(desired, TriggerGenerator.triggerNames(TABLE),
                installedAsSqliteStoresIt(TABLE, columns))).isTrue();
    }

    @Test
    void matchesRegardlessOfWhitespaceLayout() {
        List<String> columns = List.of("id", "name");
        List<String> desired = TriggerGenerator.createTriggers(TABLE, columns);

        Map<String, String> installed = new LinkedHashMap<>();
        installedAsSqliteStoresIt(TABLE, columns).forEach(
                (name, sql) -> installed.put(name, sql.replaceAll("\\s+", " ")));

        assertThat(TriggerReconciler.triggersMatch(desired, TriggerGenerator.triggerNames(TABLE), installed)).isTrue();
    }

    @Test
    void differsWhenAColumnWasAdded() {
        List<String> desired = TriggerGenerator.createTriggers(TABLE, List.of("id", "name", "discount"));

        assertThat(TriggerReconciler.triggersMatch(desired, TriggerGenerator.triggerNames(TABLE),
                installedAsSqliteStoresIt(TABLE, List.of("id", "name")))).isFalse();
    }

    @Test
    void differsWhenATriggerIsMissing() {
        List<String> columns = List.of("id", "name");
        List<String> desired = TriggerGenerator.createTriggers(TABLE, columns);

        Map<String, String> installed = installedAsSqliteStoresIt(TABLE, columns);
        installed.remove(TriggerGenerator.triggerNames(TABLE).get(0));

        assertThat(TriggerReconciler.triggersMatch(desired, TriggerGenerator.triggerNames(TABLE), installed)).isFalse();
    }

    /** The trigger SQL as SQLite records it: the generated statements with {@code IF NOT EXISTS} stripped. */
    private static Map<String, String> installedAsSqliteStoresIt(String table, List<String> columns) {
        List<String> names = TriggerGenerator.triggerNames(table);
        List<String> created = TriggerGenerator.createTriggers(table, columns);
        Map<String, String> installed = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) {
            installed.put(names.get(i), created.get(i).replaceFirst("(?i)CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER "));
        }
        return installed;
    }
}
