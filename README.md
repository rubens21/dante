# dante

PostgreSQL backup runner (`pg_backup.py`): multi-server dumps with optional Healthchecks.io pings.

## Docker

The image contains Python and `pg_dump` / `pg_dumpall` (PostgreSQL **17** client from PGDG). The client major version must be **greater than or equal to** your server major version; rebuild or adjust the Dockerfile if you target a newer server. **`backup.conf.toml` is not baked into the image**; mount it at run time.

Build (from this repository root):

```bash
docker build -t dante-pg-backup .
```

Run with your config file on the host (adjust host paths as needed). The image expects the config inside the container at `/config/backup.conf.toml` (the default `BACKUP_CONFIG`):

```bash
docker run --rm \
  -v ./backup.conf.toml:/config/backup.conf.toml:ro \
  -v ./backups:/backups/postgres \
  -v ./logs:/var/log \
  dante-pg-backup
```

- Replace `/absolute/path/on/host/backup.conf.toml` with the real path to your TOML config.
- Mount a host directory for `settings.backup_dir` from your config (the example `backup.conf.toml.example` uses `/backups/postgres`).
- If Postgres runs on the host machine, you may need `--add-host=host.docker.internal:host-gateway` (Linux Docker 20.10+) and set `host` in the config to `host.docker.internal`, or use your Docker network / host IP so the container can reach the database.

Override config path:

```bash
docker run --rm \
  -e BACKUP_CONFIG=/my/other/path.toml \
  -v /absolute/path/on/host/other.toml:/my/other/path.toml:ro \
  dante-pg-backup
```

Ensure `settings.log_file` in the config points to a writable path (e.g. a mounted volume) if logging fails inside the container.

## Backup outputs

- **`*_globals_*.sql.gz`** — Cluster globals only (`pg_dumpall --globals-only`): roles, passwords, memberships. It does **not** contain databases, tables, or data. That is expected.
- **`*_<db>_*.dump.gz`** — Full logical backup per database (custom `-Fc`). Restore with `pg_restore`.
- **`*_<db>_*.sql.gz`** — Optional full plain-SQL backup per database; set `dump_plain_sql = true` on that server in `backup.conf.toml` if you want human-readable SQL alongside the custom dump.

# Creating a backup user

```sql
-- ── Run as superuser (postgres) ───────────────────────────────────────────────
-- Creates the backup user and grants all necessary permissions.
-- Run once per cluster, then repeat the per-DB section for each database.

-- ── 1. Create user ────────────────────────────────────────────────────────────
CREATE USER backuper WITH
  PASSWORD 'change_me_to_a_strong_password'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  NOINHERIT
  LOGIN;

-- ── 2. Cluster-level read roles (Postgres 14+) ────────────────────────────────
GRANT pg_read_all_settings TO backuper;
GRANT pg_read_all_data TO backuper;

-- Needed for pg_dumpall to read role/password info from pg_authid
GRANT pg_read_all_settings TO backuper WITH ADMIN OPTION;

-- On Postgres 10–13, pg_read_all_data doesn't exist — use this instead:
-- GRANT SELECT ON pg_authid TO backuper;

-- ── 3. Per-database permissions ───────────────────────────────────────────────
-- Repeat this block for EACH database you want to back up,
-- changing the \c and database name each time.

\c lugo_vps

GRANT CONNECT ON DATABASE lugo_vps TO backuper;
GRANT USAGE ON ALL SCHEMAS IN DATABASE lugo_vps TO backuper;  -- if multiple schemas
GRANT USAGE ON SCHEMA public TO backuper;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO backuper;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO backuper;

-- Covers tables/sequences created in the future
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO backuper;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO backuper;

-- ── 4. Verify ─────────────────────────────────────────────────────────────────
\c postgres

SELECT rolname, rolinherit, rolsuper, rolcanlogin
FROM pg_roles
WHERE rolname = 'backuper';

SELECT r.rolname AS granted_role
FROM pg_auth_members m
JOIN pg_roles r ON r.oid = m.roleid
JOIN pg_roles u ON u.oid = m.member
WHERE u.rolname = 'backuper';
```