/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.sql.SQLException;
import java.util.ArrayList;
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
     * @return the names of the tables whose triggers were rebuilt, empty when nothing changed
     * @throws SQLException if the triggers cannot be read or rebuilt
     */
    public static List<String> reconcile(SQLiteConnection connection, SQLiteDatabaseSchema schema) throws SQLException {
        Map<String, String> installed = connection.readConnectorTriggerSql();
        List<String> rebuilt = new ArrayList<>();
        for (TableId tableId : schema.tableIds()) {
            String table = tableId.table();
            List<String> columns = TriggerInstaller.readColumnNames(connection, table);
            List<String> desired = TriggerGenerator.createTriggers(table, columns);
            if (!triggersMatch(desired, TriggerGenerator.triggerNames(table), installed)) {
                TriggerInstaller.rebuild(connection, table);
                rebuilt.add(table);
                LOGGER.info("Rebuilt the capture triggers for table '{}' after a schema change", table);
            }
        }
        return rebuilt;
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
