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

/**
 * Verifies that {@link TriggerReconciler#reconcile} classifies each table it touches as created,
 * altered, or dropped, on top of the trigger rebuild {@link TriggerReconcilerTest} already covers.
 */
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
        // A plain DROP TABLE takes its triggers with it, so there is no orphaned trigger to notice; the
        // table's absence from the previously-monitored set is the only signal.
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
        // RENAME TO leaves the old name's triggers attached to the renamed table, an orphan the
        // reconciler drops separately; the new name is a fresh table as far as reconcile is concerned.
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
            return TriggerReconciler.reconcile(connection, schema, previouslyMonitoredTables);
        }
    }
}
