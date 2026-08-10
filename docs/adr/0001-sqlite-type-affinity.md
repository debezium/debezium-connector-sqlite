# ADR 0001: SQLite type affinity

## Status

Accepted.

## Context

SQLite uses dynamic typing. A column has a declared type written in the schema, but SQLite does not
enforce it. Instead it derives one of five type affinities from the declared type and uses that
affinity to decide how values are stored.

The connector reads column metadata through the `org.xerial` SQLite JDBC driver, and the type the
driver reports ignores affinity. It returns a `BLOB` column and a column with no declared type as
`VARCHAR`, and a `BOOLEAN` as `INTEGER`. The connector needs a stable JDBC type per column that is
consistent with the column's affinity, so the schema builder and the value converter resolve a
column the same way regardless of what the driver reports.

## Decision

Resolve each column's affinity from its declared type using SQLite's five ordered, case-insensitive
substring rules, then map each affinity to a JDBC type.

The rules, applied in order against the upper-cased declared type:

1. contains `INT`: INTEGER
2. else contains `CHAR`, `CLOB`, or `TEXT`: TEXT
3. else contains `BLOB`, or the declared type is empty: BLOB
4. else contains `REAL`, `FLOA`, or `DOUB`: REAL
5. else: NUMERIC

The rules are substring matches, not equality. So `POINT` resolves to INTEGER because it contains
`INT`, and `BOOLEAN`, `DATE`, `DATETIME`, and `DECIMAL` all fall through to NUMERIC. A null or blank
declared type resolves to BLOB, matching SQLite's treatment of a column declared with no type.

Each affinity maps to a JDBC type:

| Affinity | JDBC type    |
| -------- | ------------ |
| INTEGER  | `BIGINT`     |
| TEXT     | `VARCHAR`    |
| BLOB     | `VARBINARY`  |
| REAL     | `DOUBLE`     |
| NUMERIC  | `NUMERIC`    |

See SQLite datatypes, section 3.1, at https://www.sqlite.org/datatype3.html.

## Consequences

The rules live in one place in the code, `SQLiteTypeAffinity.of(String)`, with the affinity to JDBC
type mapping on `SQLiteTypeAffinity.jdbcType()`. `SQLiteConnection.overrideColumn` applies the
corrected JDBC type to each column read from the driver.

The correction is defensive. The schema builder and the value converter both resolve the affinity
from the declared type string directly, so they do not depend on the corrected JDBC type. The
override keeps a column's JDBC type consistent with its affinity for any framework path that falls
back to the driver-reported type.
