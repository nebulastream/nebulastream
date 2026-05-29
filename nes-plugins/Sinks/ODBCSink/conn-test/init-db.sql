-- Bind-mounted into /docker-entrypoint-initdb.d in the `db` service. Postgres
-- runs every *.sql there on first boot before opening its port, so pg_isready /
-- `--wait` only pass once this script completes.
--
-- The ODBC sink, unlike the ODBC source, needs no pre-existing template table:
-- the loader's prepare() creates a fresh per-test target table before each run,
-- and the sink INSERTs into it. This script therefore only has to exist (so the
-- compose bind-mount resolves and the entrypoint runs cleanly); the role and
-- database are created from the POSTGRES_* environment, not here.
SELECT 1;
