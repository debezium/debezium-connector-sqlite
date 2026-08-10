# ADR 0002: Value conversion and affinity mismatch

## Status

Accepted.

## Context

SQLite uses dynamic typing, so a value's storage class can differ from the affinity of the column it
sits in. A column with INTEGER affinity can hold a text value, a blob can sit in a text column, and
so on. The connector builds a fixed Kafka Connect schema per column from the column's affinity (see
[ADR 0001](0001-sqlite-type-affinity.md)), so at read time a value whose storage class does not fit
that schema cannot be converted.

The connector needs a defined outcome for that mismatch that respects the framework's failure
handling and does not silently corrupt data. It also needs a way to keep a stream flowing past a
mismatch on a non-nullable column that has no default, where leaving the field null is not an option.

## Decision

Convert each value by the column's affinity, and handle a storage-class mismatch through the
framework's failure mode and a connector option. The flow for one value:

1. The value is null: the field is null.
2. The value's storage class fits the column's affinity: convert it to the schema's type (INTEGER to
   `Long`, REAL and NUMERIC to `Double`, TEXT to `String`, BLOB to `byte[]`).
3. The storage class does not fit: the converter throws, and `event.converting.failure.handling.mode`
   decides the outcome:
   - `fail`: stop the connector.
   - `warn`: log the column and leave the field null.
   - `skip`: leave the field null quietly.

Leaving the field null then resolves against the column:

- Nullable column: the field stays null.
- Non-nullable column with a default: the schema default takes over.
- Non-nullable column with no default: there is no null to fall back on. The connector option
  `nonnull.affinity.mismatch.fallback` decides:
  - enabled: substitute a type placeholder, `0` for INTEGER, `0.0` for REAL and NUMERIC, an empty
    string for TEXT, and empty bytes for BLOB, so the value keeps flowing.
  - disabled: the field stays null and the record fails downstream at struct validation.

The placeholder applies only under `warn` or `skip`. Under `fail` the mismatch stops the connector
before the placeholder is considered. The connector resolves this gating from the failure mode and
the option, and passes a single boolean to `SQLiteValueConverter`, which substitutes the placeholder
for a non-nullable, no-default column and otherwise throws.

## Consequences

The behavior lives in `SQLiteValueConverter`. `converter(Column, Field)` returns a per-affinity
function, and `convert` passes null through, converts a value whose storage class fits, substitutes a
placeholder for a non-nullable no-default column when the fallback is enabled, and otherwise throws a
`DebeziumException` describing the mismatch.

The mismatch outcome is not decided in the converter alone. It depends on the framework failure mode
and the connector option, so the same value can stop the connector, drop to null, take a default, or
become a placeholder depending on configuration. The placeholder keeps a stream flowing at the cost
of a synthetic value in place of the real one, which is why it is off by default and gated behind a
non-failing mode.
