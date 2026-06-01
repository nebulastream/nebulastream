-- Bind-mounted into /docker-entrypoint-initdb.d in the `db` service.
-- Postgres runs every *.sql there on first boot before opening its port,
-- so pg_isready / `--wait` only pass once this template table exists and
-- the loader's `CREATE TABLE IF NOT EXISTS ...` for per-test tables can
-- resolve. The schema mirrors the loader's per-test tables: the projected
-- columns plus the `created_at` windowing column the ODBC source filters
-- on (WHERE created_at > ? AND created_at <= ?).
CREATE TABLE conntest_rows (
    k BIGINT PRIMARY KEY,                       -- INT64   -> 8 bytes
    a INTEGER NOT NULL,                         -- INT32   -> 4 bytes
    b BIGINT NOT NULL,                          -- INT64   -> 8 bytes
    c DOUBLE PRECISION NOT NULL,                -- FLOAT64 -> 8 bytes
    created_at TIMESTAMP NOT NULL DEFAULT now() -- windowing column (not projected)
);
