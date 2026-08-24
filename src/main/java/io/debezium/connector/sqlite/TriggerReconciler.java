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
 * Brings the installed capture triggers back in step with the current schema.
 *
 * <p>The trigger SQL is a pure function of a table's name and column list, so the reconciler does not
 * need to know which DDL statement ran. For each monitored table it generates the trigger SQL it wants
 * and compares it against the SQL actually installed; where they differ, it rebuilds. One rule covers a
 * column added, dropped, or renamed, and a table whose triggers are missing entirely.
 *
 * <p>It also drops connector triggers left behind for a table no longer monitored. An
 * {@code ALTER TABLE ... RENAME TO} leaves the old name's triggers attached to the renamed table, still
 * firing, so without this cleanup every write would be captured twice.
 */
public final class TriggerReconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerReconciler.class);

    private TriggerReconciler() {
    }

    /**
     * Rebuilds the triggers of every monitored table whose installed triggers no longer match its
     * columns, leaving the rest untouched.
     *
     * @param connection an open connection to the SQLite database
     * @param schema the connector's schema, refreshed to the current state
     * @param previouslyMonitoredTables the table names monitored before this reconcile, so a table gone
     *        from the schema can be reported dropped even when it left no orphaned trigger behind, as a
     *        plain {@code DROP TABLE} does not (SQLite drops its triggers along with it)
     * @return the tables that were created, altered, or dropped; empty lists when nothing changed
     * @throws SQLException if the triggers cannot be read or rebuilt
     */
    public static ReconcileResult reconcile(SQLiteConnection connection, SQLiteDatabaseSchema schema,
                                            Set<String> previouslyMonitoredTables)
            throws SQLException {
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
        for (String orphan : orphanedTriggers(installed.keySet(), monitoredTables)) {
            connection.execute("DROP TRIGGER IF EXISTS " + orphan);
            LOGGER.info("Dropped orphaned capture trigger '{}' left by a table that is no longer monitored", orphan);
        }
        List<String> dropped = previouslyMonitoredTables.stream()
                .filter(table -> !monitoredTables.contains(table))
                .collect(Collectors.toList());
        return new ReconcileResult(created, altered, dropped);
    }

    /**
     * The connector triggers that no longer belong to any monitored table, so they should be dropped.
     * The expected triggers are the ones every monitored table should have; an installed connector
     * trigger outside that set is an orphan, as after a rename. Only connector-prefixed names are
     * returned, so a user's own trigger is never treated as an orphan even if it were passed in.
     */
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

    /**
     * Whether a table's installed triggers already match the ones it should have. The desired
     * statements carry {@code IF NOT EXISTS} and SQLite stores the installed SQL without it, so both
     * sides are normalized before comparing. A missing trigger, or one built from a different column
     * list, makes the sets differ.
     */
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
     * Reduces a {@code CREATE TRIGGER} statement to a form that compares equal whether or not it
     * carries {@code IF NOT EXISTS} and regardless of how its whitespace is laid out. SQLite preserves
     * the trigger text but strips {@code IF NOT EXISTS}, so removing that clause and collapsing
     * whitespace lets the generated SQL compare equal to the stored SQL.
     */
    private static String normalize(String createTriggerSql) {
        String collapsed = createTriggerSql.replaceAll("\\s+", " ").trim();
        return collapsed.replaceFirst("(?i)CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER ");
    }
}
