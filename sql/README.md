# SQL Models

SQL is organized by data layer:

- `ddl`: schemas, tables, constraints, and indexes.
- `staging`: technical cleaning and standardization.
- `core`: reusable facts and dimensions.
- `marts`: stakeholder-oriented analytical tables.
- `tests`: data-quality assertions.

`ddl/001_initialize.sql` creates the initial append-only PostgreSQL model.
Docker runs files in `ddl` only while initializing a new database volume.
Run `tests/postgres_smoke_test.sql` after startup to verify the required
relations exist.
