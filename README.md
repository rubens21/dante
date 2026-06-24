# dante

Multi-source backup runner (`backup.py`): PostgreSQL dumps, S3 pulls, and optional upload to an S3 destination — with Healthchecks.io monitoring.

## Docker

The image contains Python, `pg_dump` / `pg_dumpall` (PostgreSQL **17** client from PGDG), and the **AWS CLI** (for `aws s3 sync`). The client major version must be **greater than or equal to** your server major version; rebuild or adjust the Dockerfile if you target a newer server. **`backup.conf.toml` is not baked into the image**; mount it at run time.

Build (from this repository root):

```bash
docker build -t dante-backup .
```

Run with your config file on the host (adjust host paths as needed). The image expects the config inside the container at `/config/backup.conf.toml` (the default `BACKUP_CONFIG`):

```bash
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -v ./backup.conf.toml:/config/backup.conf.toml:ro \
  -v ./backups:/backups/postgres \
  -v ./logs:/var/log \
  dante-backup
```

On Linux, `host.docker.internal` is **not** available unless you pass `--add-host=host.docker.internal:host-gateway` (shown above). Without it, S3/Garage connections to the host will fail.

If Garage runs in Docker on the same machine, you can instead join its compose network and point at the service name:

```bash
docker run --rm \
  --network garage_default \
  -v ./backup.conf.toml:/config/backup.conf.toml:ro \
  -v ./backups:/backups/postgres \
  -v ./logs:/var/log \
  dante-backup
```

Set `endpoint_url = "http://garage:3900"` in `[s3]` when using this approach.

- Replace `/absolute/path/on/host/backup.conf.toml` with the real path to your TOML config.
- Mount a host directory for `settings.backup_dir` from your config (the example `backup.conf.toml.example` uses `/backups/postgres`).
- If Postgres runs on the host machine, you may need `--add-host=host.docker.internal:host-gateway` (Linux Docker 20.10+) and set `host` in the config to `host.docker.internal`, or use your Docker network / host IP so the container can reach the database.

For S3 or Garage, pass credentials via config or environment variables when not using an IAM role:

```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID=GK... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -v ./backup.conf.toml:/config/backup.conf.toml:ro \
  -v ./backups:/backups/postgres \
  -v ./logs:/var/log \
  dante-backup
```

Override config path:

```bash
docker run --rm \
  -e BACKUP_CONFIG=/my/other/path.toml \
  -v /absolute/path/on/host/other.toml:/my/other/path.toml:ro \
  dante-backup
```

Ensure `settings.log_file` in the config points to a writable path (e.g. a mounted volume) if logging fails inside the container.

## Backup outputs

### PostgreSQL

- **`*_globals_*.sql.gz`** — Cluster globals only (`pg_dumpall --globals-only`): roles, passwords, memberships. It does **not** contain databases, tables, or data. That is expected.
- **`*_<db>_*.dump.gz`** — Full logical backup per database (custom `-Fc`). Restore with `pg_restore`.
- **`*_<db>_*.sql.gz`** — Optional full plain-SQL backup per database; set `dump_plain_sql = true` on that server in `backup.conf.toml` if you want human-readable SQL alongside the custom dump.

### S3 sources

Each `[[s3_sources]]` entry pulls remote objects into `{backup_dir}/{name}_{timestamp}/`, mirroring the bucket prefix tree. Local retention (`settings.retention_days`) prunes both files and directories.

With `all_buckets = true`, the runner calls `aws s3 ls` (works with Garage and AWS), then syncs every listed bucket into `{name}_{timestamp}/{bucket}/`. Use `exclude_buckets` to skip buckets you do not want backed up.

## S3 / Garage connection

S3 blocks must identify **which S3 server** to talk to. This is required for [Garage](https://garagehq.deuxfleurs.fr/) and other S3-compatible stores, not only AWS.

Optional shared defaults in `[s3]` (inherited by `[[s3_sources]]` and `[s3_destination]`):

| Field | Description |
|-------|-------------|
| `endpoint_url` | S3 API base URL (e.g. `https://garage.example.com` or `http://127.0.0.1:3900`). Omit for real AWS. |
| `region` | **Required.** Garage cluster region name (often `garage`) or an AWS region (e.g. `eu-west-1`). |
| `access_key_id` | Optional; omit to use env vars or IAM role. |
| `secret_access_key` | Optional; omit to use env vars or IAM role. |
| `addressing_style` | Optional: `path`, `virtual`, or `auto` (default). Use `path` for Garage if virtual-host style fails. |
| `verify_ssl` | Optional; set `false` for self-signed TLS (e.g. local Garage without TLS). |

Credential resolution order: block fields → `[s3]` section → `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars → IAM role (AWS only).

Example Garage config (`backup.conf.toml.example`):

```toml
[s3]
endpoint_url = "http://127.0.0.1:3900"
region = "garage"
access_key_id = "GK..."
secret_access_key = "..."
addressing_style = "path"
verify_ssl = false
```

For AWS-native usage, omit `endpoint_url` and set `region` to an AWS region on `[s3]` or each block.

## S3 sources (pull)

**All buckets** (Garage or AWS — key must be able to list buckets):

```toml
[[s3_sources]]
name = "garage-all"
all_buckets = true
exclude_buckets = ["scratch"]    # optional
```

Output layout: `{backup_dir}/garage-all_{timestamp}/{bucket}/...`

**Single bucket:**

```toml
[[s3_sources]]
name = "assets"
bucket = "my-assets-bucket"
prefix = "uploads/"    # optional, default ""
```

Each source needs `name`. Set either `bucket` or `all_buckets = true` (not both). `region` is required (on the block or in `[s3]`). Per-source fields override `[s3]` defaults. Missing or empty `[[s3_sources]]` is a no-op — Postgres-only configs keep working.

The backup key needs **list** access (`aws s3 ls`) for discovery and **read** access (`aws s3 sync`) on each bucket. On Garage, grant the key permissions on the buckets you want included.

## S3 destination (push)

```toml
[s3_destination]
bucket = "my-backup-archive"
prefix = "dante/"      # optional key prefix
```

When `[s3_destination]` is present (requires `bucket`), the runner uploads the full `backup_dir` via `aws s3 sync` after all Postgres dumps and S3 pulls succeed, and **before** local retention pruning.

- **AWS:** the IAM principal needs `s3:PutObject`, `s3:ListBucket`, and `s3:GetObject` on the destination bucket/prefix.
- **Garage:** the bucket must exist and the key must have read/write access.

S3-side retention is out of scope — use bucket lifecycle rules on AWS or Garage admin tools instead.

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
