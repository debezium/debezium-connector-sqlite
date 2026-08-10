# Debezium SQLite Connector

A Debezium source connector for SQLite. It captures row-level changes from a SQLite database file
through a change-data-capture log table that triggers keep up to date, snapshots the existing data,
and streams new changes as they are committed.

This connector is under active development.

## Architecture decisions

- [ADR 0001: SQLite type affinity](docs/adr/0001-sqlite-type-affinity.md)
- [ADR 0002: Value conversion and affinity mismatch](docs/adr/0002-value-conversion-and-affinity-mismatch.md)
