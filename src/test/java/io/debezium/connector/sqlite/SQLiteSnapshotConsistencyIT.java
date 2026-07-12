/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Proves the consistency the snapshot resume point relies on. The snapshot reads its high-water mark as
 * the first statement of a transaction with auto-commit off, which in SQLite WAL mode opens a consistent
 * read view. A change committed by another connection after that read is invisible to the view, so it is
 * excluded from the watermark and from the snapshot data, and is left for streaming. This reproduces the
 * snapshot's own sequence with two connections, so it needs a real database and runs under Failsafe.
 */
public class SQLiteSnapshotConsistencyIT {

    @Test
    void readViewExcludesChangesCommittedAfterTheWatermark() throws Exception {
        try (SqliteTestHelper db = SqliteTestHelper.create();
                SQLiteConnection snapshot = new SQLiteConnection(db.databaseFile().toString());
                SQLiteConnection writer = new SQLiteConnection(db.databaseFile().toString())) {
            snapshot.connect();
            writer.connect();

            writer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
            writer.execute("INSERT INTO t (id) VALUES (1)");
            insertCdcLogRow(writer, 1);

            // Open the read view exactly as the snapshot does: auto-commit off, then read the watermark
            // as the first statement.
            snapshot.connection().setAutoCommit(false);
            assertThat(snapshot.readMaxChangeId()).isEqualTo(1L);

            // A concurrent connection commits a new change and a new row after the view opened.
            insertCdcLogRow(writer, 2);
            writer.execute("INSERT INTO t (id) VALUES (2)");

            // The snapshot's view still sees the old watermark and not the new row, so the new change is
            // left for streaming.
            assertThat(snapshot.readMaxChangeId()).isEqualTo(1L);
            assertThat(readIds(snapshot)).containsExactly(1L);

            snapshot.connection().rollback();
        }
    }

    private static void insertCdcLogRow(SQLiteConnection connection, long changeId) throws SQLException {
        connection.execute(String.format(
                "INSERT INTO %s (%s, %s, %s, %s) VALUES (%d, 't', '%s', 0)",
                CdcLog.TABLE_NAME, CdcLog.CHANGE_ID, CdcLog.TABLE_NAME_COLUMN, CdcLog.OPERATION,
                CdcLog.COMMITTED_AT, changeId, CdcLog.OPERATION_CREATE));
    }

    private static List<Long> readIds(SQLiteConnection connection) throws SQLException {
        return connection.queryAndMap("SELECT id FROM t ORDER BY id", rs -> {
            List<Long> ids = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
            return ids;
        });
    }
}
