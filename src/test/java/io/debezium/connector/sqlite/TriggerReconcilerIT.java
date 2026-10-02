/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

/** Verifies that {@link TriggerReconciler#reconcile} classifies tables as created, altered, or dropped. */
public class TriggerReconcilerIT {

    @Test
    void reportsATableWithNoInstalledTriggersAsCreated() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");

            ReconcileResult result = reconcile(db, Set.of());

            assertThat(result.created()).containsExactly("orders");
            assertThat(result.altered()).isEmpty();
            assertThat(result.dropped()).isEmpty();
        }
    }

    @Test
    void reportsATableWithMismatchedTriggersAsAltered() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
            db.installTriggers("orders");
            db.connection().execute("ALTER TABLE orders ADD COLUMN note TEXT");

            ReconcileResult result = reconcile(db, Set.of("orders"));

            assertThat(result.altered()).containsExactly("orders");
            assertThat(result.created()).isEmpty();
            assertThat(result.dropped()).isEmpty();
        }
    }

    @Test
    void reportsADroppedTableAsDroppedEvenWithNoOrphanedTriggersLeftBehind() throws Exception {
        // DROP TABLE removes its triggers too, so only the previously-monitored set reveals it.
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
            db.installTriggers("orders");
            db.connection().execute("DROP TABLE orders");

            ReconcileResult result = reconcile(db, Set.of("orders"));

            assertThat(result.dropped()).containsExactly("orders");
            assertThat(result.created()).isEmpty();
            assertThat(result.altered()).isEmpty();
        }
    }

    @Test
    void reportsARenamedAwayTableAsDroppedAndTheNewNameAsCreated() throws Exception {
        // RENAME TO keeps the old name's triggers on the table; the new name counts as created.
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
            db.installTriggers("orders");
            db.connection().execute("ALTER TABLE orders RENAME TO sales_orders");

            ReconcileResult result = reconcile(db, Set.of("orders"));

            assertThat(result.dropped()).containsExactly("orders");
            assertThat(result.created()).containsExactly("sales_orders");
            assertThat(result.altered()).isEmpty();
        }
    }

    @Test
    void reportsNothingWhenEveryTableAlreadyMatches() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create()) {
            db.connection().execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
            db.installTriggers("orders");

            ReconcileResult result = reconcile(db, Set.of("orders"));

            assertThat(result.created()).isEmpty();
            assertThat(result.altered()).isEmpty();
            assertThat(result.dropped()).isEmpty();
        }
    }

    private ReconcileResult reconcile(SqliteTestHelper db, Set<String> previouslyMonitoredTables) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(SQLiteConnectorConfig.DATABASE_FILE.name(), db.databaseFile().toString());
        props.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "test");
        Configuration config = Configuration.from(props);
        SQLiteConnectorConfig connectorConfig = new SQLiteConnectorConfig(config);
        CdcSourceTaskContext<SQLiteConnectorConfig> taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        SQLiteDatabaseSchema schema = new SQLiteDatabaseSchema(taskContext, topicNamingStrategy);

        try (SQLiteConnection connection = new SQLiteConnection(db.databaseFile().toString())) {
            schema.refresh(connection);
            return TriggerReconciler.reconcile(connection, connectorConfig.getTableFilters().dataCollectionFilter(), previouslyMonitoredTables);
        }
    }
}
