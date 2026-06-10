/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Arrays;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/**
 * Configuration for the SQLite connector.
 *
 * <p>Add connector-specific {@link Field} constants here. Each field must be documented
 * and included in {@link #ALL_FIELDS} and {@link #configDef()}.
 */
public class SQLiteConnectorConfig extends RelationalDatabaseConnectorConfig {

    /**
     * Snapshot mode controlling whether an initial snapshot is performed on startup.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /** Snapshot existing data when no offset is stored; resume streaming otherwise. */
        INITIAL("initial") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return !offsetExists || snapshotInProgress;
            }
        },

        /** Skip the initial snapshot and start streaming from the current CDC log position. */
        NEVER("never") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return false;
            }
        };

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /** Returns true if a snapshot should be performed given the current offset state. */
        public abstract boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress);

        /** Parses the string value into a {@code SnapshotMode}, ignoring case. */
        public static SnapshotMode parse(String value) {
            return Arrays.stream(values())
                    .filter(m -> m.value.equalsIgnoreCase(value))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown snapshot mode: " + value));
        }
    }

    /** Path to the SQLite database file that the connector will monitor. */
    public static final Field DATABASE_FILE = Field.create("database.dbname")
            .withDisplayName("Database file path")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Path to the SQLite database file to monitor.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specifies the criteria for performing a snapshot on startup. "
                    + "Options include: 'initial' (default) to snapshot only when no offset exists; "
                    + "'never' to skip snapshot entirely.");

    public static final Field.Set ALL_FIELDS = Field.setOf(
            CommonConnectorConfig.TOPIC_PREFIX,
            DATABASE_FILE,
            SNAPSHOT_MODE,
            RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
            RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
            RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST,
            RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST);

    /** Tables that are always excluded from monitoring (SQLite internals and the CDC log itself). */
    private static final TableFilter SYSTEM_TABLES_FILTER = TableFilter.fromPredicate(t -> t.table().startsWith("sqlite_")
            || t.table().equals("_debezium_cdc_log"));

    /** Maps a {@code TableId} to its string form for include/exclude list matching. */
    private static final TableIdToStringMapper TABLE_ID_MAPPER = TableId::table;

    private final SnapshotMode snapshotMode;

    public SQLiteConnectorConfig(Configuration config) {
        super(config, SYSTEM_TABLES_FILTER, TABLE_ID_MAPPER, 2048, ColumnFilterMode.CATALOG, false);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
    }

    public static ConfigDef configDef() {
        ConfigDef configDef = new ConfigDef();
        Field.group(configDef, "SQLite", DATABASE_FILE);
        Field.group(configDef, "Connector", CommonConnectorConfig.TOPIC_PREFIX);
        Field.group(configDef, "Snapshots", SNAPSHOT_MODE);
        Field.group(configDef, "Filtering",
                RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
                RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST,
                RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST);
        return configDef;
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        SQLiteSourceInfoStructMaker maker = new SQLiteSourceInfoStructMaker();
        maker.init(Module.name(), Module.version(), this);
        return maker;
    }
}
