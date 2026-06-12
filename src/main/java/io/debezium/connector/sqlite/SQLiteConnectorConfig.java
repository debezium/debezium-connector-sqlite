/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Arrays;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.ConfigDefinition;
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
 * <p>Add connector-specific {@link Field} constants here. Each field must be documented and
 * registered with {@link #CONFIG_DEFINITION}, which derives both {@link #ALL_FIELDS} and
 * {@link #configDef()} so the field set cannot drift between them.
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
    public static final Field DATABASE_FILE = Field.create("database.file.path")
            .withDisplayName("Database file path")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Path to the SQLite database file to monitor.");

    /** Whether to take an initial snapshot of existing table data before streaming changes. */
    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specifies the criteria for performing a snapshot on startup. "
                    + "Options include: 'initial' (default) to snapshot only when no offset exists; "
                    + "'never' to skip snapshot entirely.");

    /**
     * The connector's configuration, built on the relational base definition. {@code type} fields
     * describe how to reach the database, {@code connector} fields tune behavior, and SQLite has no
     * schemas so the schema include and exclude lists are excluded.
     */
    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("SQLite")
            .type(DATABASE_FILE)
            .connector(SNAPSHOT_MODE)
            .excluding(SCHEMA_INCLUDE_LIST, SCHEMA_EXCLUDE_LIST)
            .create();

    /** The full set of fields the connector accepts, derived from {@link #CONFIG_DEFINITION}. */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

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
        return CONFIG_DEFINITION.configDef();
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
