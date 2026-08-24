/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.TableId;

/**
 * Brings the installed capture triggers back in step with the current schema. The trigger SQL is a pure
 * function of a table's name and columns, so for each monitored table the reconciler regenerates the SQL
 * and rebuilds where it differs from what is installed, covering a column added, dropped, or renamed and
 * triggers missing entirely. It also drops connector triggers left on a renamed table, which would
 * otherwise capture every write twice.
 */
public final class TriggerReconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerReconciler.class);

    private TriggerReconciler() {
    }

    /**
     * Rebuilds the triggers of every monitored table whose installed triggers no longer match its
     * columns, and reports the tables created, altered, or dropped.
     */
    public static ReconcileResult reconcile(SQLiteConnection connection, SQLiteDatabaseSchema schema) throws SQLException {
        Map<String, String> installed = connection.readConnectorTriggerSql();
        Set<String> monitoredTables = new LinkedHashSet<>();
        List<String> created = new ArrayList<>();
        List<String> altered = new ArrayList<>();
        for (TableId tableId : schema.tableIds()) {
            String table = tableId.table();
            monitoredTables.add(table);
            List<String> triggerNames = TriggerGenerator.triggerNames(table);
            boolean hadInstalledTriggers = triggerNames.stream().anyMatch(installed::containsKey);
            List<String> columns = TriggerInstaller.readColumnNames(connection, table);
            List<String> desired = TriggerGenerator.createTriggers(table, columns);
            if (!triggersMatch(desired, triggerNames, installed)) {
                TriggerInstaller.rebuild(connection, table);
                (hadInstalledTriggers ? altered : created).add(table);
                LOGGER.info("Rebuilt the capture triggers for table '{}' after a schema change", table);
            }
        }
        List<String> dropped = new ArrayList<>();
        for (String orphan : orphanedTriggers(installed.keySet(), monitoredTables)) {
            connection.execute("DROP TRIGGER IF EXISTS " + orphan);
            LOGGER.info("Dropped orphaned capture trigger '{}' left by a table that is no longer monitored", orphan);
            TriggerGenerator.tableNameFor(orphan).ifPresent(dropped::add);
        }
        return new ReconcileResult(created, altered, dropped.stream().distinct().collect(Collectors.toList()));
    }

    static List<String> orphanedTriggers(Set<String> installedTriggerNames, Set<String> monitoredTables) {
        Set<String> expected = monitoredTables.stream()
                .flatMap(table -> TriggerGenerator.triggerNames(table).stream())
                .collect(Collectors.toSet());
        return installedTriggerNames.stream()
                .filter(name -> name.startsWith(TriggerGenerator.TRIGGER_PREFIX))
                .filter(name -> !expected.contains(name))
                .sorted()
                .collect(Collectors.toList());
    }

    static boolean triggersMatch(List<String> desiredCreateStatements, List<String> triggerNames,
                                 Map<String, String> installedSql) {
        Set<String> desired = desiredCreateStatements.stream()
                .map(TriggerReconciler::normalize)
                .collect(Collectors.toSet());
        Set<String> installed = triggerNames.stream()
                .map(installedSql::get)
                .filter(Objects::nonNull)
                .map(TriggerReconciler::normalize)
                .collect(Collectors.toSet());
        return desired.equals(installed);
    }

    /**
     * Reduces a {@code CREATE TRIGGER} statement to a form that compares equal regardless of
     * {@code IF NOT EXISTS} or whitespace, since SQLite stores the text but strips {@code IF NOT EXISTS}.
     */
    private static String normalize(String createTriggerSql) {
        String collapsed = createTriggerSql.replaceAll("\\s+", " ").trim();
        return collapsed.replaceFirst("(?i)CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER ");
    }
}
