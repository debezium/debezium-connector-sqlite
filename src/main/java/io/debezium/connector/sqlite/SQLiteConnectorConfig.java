/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Arrays;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
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

        /** Snapshot existing data on every start, even when an offset is stored. */
        ALWAYS("always") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return true;
            }
        },

        /** Never snapshot data; start streaming from the current CDC log position. */
        NO_DATA("no_data") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return false;
            }
        },

        /** Snapshot existing data once, then stop without streaming. */
        INITIAL_ONLY("initial_only") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return !offsetExists || snapshotInProgress;
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
            .required()
            .withDescription("Path to the SQLite database file to monitor.");

    /** Default per-poll row limit when reading the CDC log table. */
    public static final int DEFAULT_CDC_LOG_BATCH_SIZE = 1000;

    /**
     * Maximum number of rows to read from {@code _debezium_cdc_log} per streaming poll. This is the
     * {@code LIMIT} on the CDC-log read, kept small so each read transaction stays short and SQLite
     * can checkpoint the WAL between polls. It is separate from the inherited {@code max.batch.size},
     * which bounds how many records the change-event queue hands to Kafka Connect per poll; the two
     * are decoupled by the queue buffer.
     */
    public static final Field CDC_LOG_BATCH_SIZE = Field.create("cdc.log.batch.size")
            .withDisplayName("CDC log batch size")
            .withType(ConfigDef.Type.INT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_CDC_LOG_BATCH_SIZE)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Maximum number of rows to read from the CDC log table per poll. "
                    + "Defaults to " + DEFAULT_CDC_LOG_BATCH_SIZE + ".");

    /**
     * Whether to substitute a type placeholder when a row's stored value does not match its column's
     * SQLite affinity and cannot be represented in the column's schema type. This only affects a
     * non-nullable column with no default, which has no null to fall back on: without it such a value
     * fails to convert, with it the connector emits a placeholder (0 for INTEGER, 0.0 for REAL and
     * NUMERIC, an empty string for TEXT, and empty bytes for BLOB). Nullable columns and columns with a
     * default are unaffected; they already resolve to null or their default.
     */
    public static final Field NONNULL_AFFINITY_MISMATCH_FALLBACK = Field.create("nonnull.affinity.mismatch.fallback")
            .withDisplayName("Substitute a placeholder for a non-nullable affinity mismatch")
            .withType(ConfigDef.Type.BOOLEAN)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("For a non-nullable column with no default, emit a type placeholder "
                    + "(0, 0.0, an empty string, or empty bytes) when a value's storage class does not "
                    + "match the column's SQLite affinity, instead of failing the conversion. Nullable "
                    + "columns and columns with a default are unaffected. Defaults to false.");

    /** Whether to take an initial snapshot of existing table data before streaming changes. */
    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specifies the criteria for performing a snapshot on startup. "
                    + "Options include: 'initial' (default) to snapshot only when no offset exists; "
                    + "'no_data' to skip the data snapshot and stream from the current position.");

    /**
     * The connector's configuration, built on the relational base definition. {@code type} fields
     * describe how to reach the database, {@code connector} fields tune behavior, and SQLite has no
     * schemas so the schema include and exclude lists are excluded.
     */
    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("SQLite")
            .type(DATABASE_FILE)
            .connector(SNAPSHOT_MODE, CDC_LOG_BATCH_SIZE, NONNULL_AFFINITY_MISMATCH_FALLBACK)
            .excluding(SCHEMA_INCLUDE_LIST, SCHEMA_EXCLUDE_LIST)
            .create();

    /** The full set of fields the connector accepts, derived from {@link #CONFIG_DEFINITION}. */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    /** Prefix reserved for the connector's internal tables, always excluded from monitoring. */
    private static final String DEBEZIUM_TABLE_PREFIX = "_debezium_";

    /**
     * Keeps user tables under monitoring and always excludes SQLite internals ({@code sqlite_}) and
     * the connector's own {@code _debezium_} tables. The framework monitors a table only when this
     * returns true, so the predicate is true for the tables to keep, not for the ones to drop.
     */
    private static final TableFilter SYSTEM_TABLES_FILTER = TableFilter.fromPredicate(t -> !t.table().startsWith("sqlite_")
            && !t.table().startsWith(DEBEZIUM_TABLE_PREFIX));

    /** Maps a {@code TableId} to its string form for include/exclude list matching. */
    private static final TableIdToStringMapper TABLE_ID_MAPPER = TableId::table;

    private final SnapshotMode snapshotMode;
    private final boolean nonNullAffinityMismatchFallbackEnabled;

    public SQLiteConnectorConfig(Configuration config) {
        super(config, SYSTEM_TABLES_FILTER, TABLE_ID_MAPPER, 2048, ColumnFilterMode.CATALOG, false);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.nonNullAffinityMismatchFallbackEnabled = config.getBoolean(NONNULL_AFFINITY_MISMATCH_FALLBACK);
    }

    /**
     * Whether to substitute a type placeholder for an affinity-mismatched value in a non-nullable,
     * no-default column. The opt-in only takes effect under the {@code warn} and {@code skip} failure
     * modes, which are the modes that would otherwise leave such a value to fail at serialization. Under
     * {@code fail} the connector is meant to stop, so no placeholder is substituted.
     */
    public boolean shouldSubstituteNonNullPlaceholder() {
        return nonNullAffinityMismatchFallbackEnabled
                && getEventConvertingFailureHandlingMode() != EventConvertingFailureHandlingMode.FAIL;
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
