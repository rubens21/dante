#!/usr/bin/env python3
"""
backup.py — multi-source backup runner (Postgres + S3) with Healthchecks.io monitoring.
Single HC check: only pings success if ALL backup steps succeed.
"""

import gzip
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# Python 3.11+: tomllib is stdlib. For 3.10 and below, install tomli:
#   pip install tomli
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        sys.exit("ERROR: Python < 3.11 detected. Run: pip install tomli")


def _probe_s3_endpoint(endpoint_url: str) -> dict:
    parsed = urlparse(endpoint_url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    probe = {"endpoint_url": endpoint_url, "host": host, "port": port}
    try:
        probe["dns_ip"] = socket.gethostbyname(host)
    except OSError as exc:
        probe["dns_error"] = str(exc)
        return probe
    try:
        socket.create_connection((host, port), timeout=3).close()
        probe["tcp_ok"] = True
    except OSError as exc:
        probe["tcp_error"] = str(exc)
    return probe


def _validate_s3_endpoint(endpoint_url: str, logger: logging.Logger) -> None:
    probe = _probe_s3_endpoint(endpoint_url)
    if probe.get("dns_error"):
        host = probe.get("host", "")
        if host == "host.docker.internal":
            logger.error(
                "Cannot resolve host.docker.internal from inside the container. "
                "On Linux, add --add-host=host.docker.internal:host-gateway "
                "to docker run. Alternatively, join the Garage compose network "
                '(e.g. --network garage_default) and set '
                'endpoint_url = "http://garage:3900".'
            )
        else:
            logger.error(
                f"Cannot resolve S3 host {host!r}: {probe['dns_error']}"
            )
        sys.exit(1)
    if not probe.get("tcp_ok"):
        logger.error(
            f"Cannot reach S3 endpoint {endpoint_url!r}: "
            f"{probe.get('tcp_error', 'unknown error')}"
        )
        sys.exit(1)
    logger.debug(
        f"S3 endpoint reachable: {endpoint_url} "
        f"({probe['host']}:{probe['port']} → {probe.get('dns_ip')})"
    )


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("dante")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ── Healthchecks ──────────────────────────────────────────────────────────────

def ping_healthchecks(base_url: str, suffix: str, logger: logging.Logger):
    """suffix: '' (success) | '/start' | '/fail'"""
    url = f"{base_url.rstrip('/')}{suffix}"
    try:
        urllib.request.urlopen(url, timeout=10)
        logger.debug(f"HC ping sent: {suffix or '(success)'}")
    except Exception as e:
        # Never let a monitoring ping failure crash the backup
        logger.warning(f"HC ping failed ({url}): {e}")


# ── Subprocess helpers ────────────────────────────────────────────────────────

def run_dump(cmd: list[str], env: dict, output_path: Path,
             logger: logging.Logger) -> tuple[bool, list[str]]:
    """
    Run a pg_dump/pg_dumpall command, streaming stdout into a gzip file.
    Returns (success: bool, errors: list[str]).

    Captures stderr separately so we can inspect it for warnings/errors
    even if the process exit code is 0 (silent failures).
    """
    errors = []

    try:
        with gzip.open(output_path, "wb") as out_file:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            stdout_data, stderr_data = proc.communicate()
            out_file.write(stdout_data)

        stderr_text = stderr_data.decode("utf-8", errors="replace")

        # ── Scan stderr for anything suspicious ───────────────────────────
        # pg_dump writes progress/notices to stderr too, so we filter:
        # treat lines with "error" or "permission denied" as real problems.
        error_patterns = re.compile(
            r"(error|permission denied|could not|fatal|invalid|failed)",
            re.IGNORECASE,
        )
        for line in stderr_text.splitlines():
            if error_patterns.search(line):
                errors.append(line.strip())
                logger.error(f"  stderr: {line.strip()}")
            else:
                logger.debug(f"  pg stderr: {line.strip()}")

        # ── Exit code check ───────────────────────────────────────────────
        if proc.returncode != 0:
            errors.append(f"Process exited with code {proc.returncode}")
            logger.error(f"  Exit code: {proc.returncode}")

        # ── Output sanity check ───────────────────────────────────────────
        # A near-empty file almost certainly means something went wrong
        # even if the exit code was 0 (e.g. connected but had no read access).
        file_size = output_path.stat().st_size
        if file_size < 100:
            errors.append(
                f"Output file suspiciously small ({file_size} bytes) — "
                "likely a permissions or connection issue"
            )
            logger.error(f"  Output file too small: {file_size} bytes")

    except Exception as e:
        errors.append(str(e))
        logger.error(f"  Exception during dump: {e}")

    return (len(errors) == 0), errors


def run_aws_command(cmd: list[str], env: dict,
                    logger: logging.Logger) -> tuple[bool, list[str]]:
    """
    Run an aws CLI command.
    Returns (success: bool, errors: list[str]).
    """
    ok, errors, _ = run_aws_capture(cmd, env, logger)
    return ok, errors


def run_aws_capture(cmd: list[str], env: dict,
                    logger: logging.Logger) -> tuple[bool, list[str], str]:
    """
    Run an aws CLI command and return captured stdout.
    Returns (success: bool, errors: list[str], stdout: str).
    """
    errors = []
    stdout = ""

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            env=env,
            text=True,
        )
        stdout = proc.stdout or ""

        if stdout:
            for line in stdout.splitlines():
                logger.debug(f"  aws stdout: {line.strip()}")

        if proc.stderr:
            for line in proc.stderr.splitlines():
                if proc.returncode != 0:
                    logger.error(f"  stderr: {line.strip()}")
                    errors.append(line.strip())
                else:
                    logger.debug(f"  aws stderr: {line.strip()}")

        if proc.returncode != 0:
            errors.append(f"Process exited with code {proc.returncode}")
            logger.error(f"  Exit code: {proc.returncode}")

    except Exception as e:
        errors.append(str(e))
        logger.error(f"  Exception during aws command: {e}")

    return (len(errors) == 0), errors, stdout


# ── Backup logic ──────────────────────────────────────────────────────────────

def backup_server(server: dict, backup_dir: Path, timestamp: str,
                  logger: logging.Logger) -> list[str]:
    """
    Runs all dumps for one server.
    Returns a list of error strings (empty = all good).
    """
    all_errors = []
    name = server["name"]
    host = server["host"]
    port = str(server.get("port", 5432))
    user = server["user"]
    password = server["password"]

    # Pass password via env — safer than command line (visible in ps aux)
    env = {**os.environ, "PGPASSWORD": password}
    common_args = ["-h", host, "-p", port, "-U", user,
                   "--clean", "--if-exists"]

    # ── Globals dump (roles, memberships only — not database DDL/data) ─────
    if server.get("dump_globals", False):
        logger.info(f"[{name}] Running pg_dumpall (globals-only)...")
        out = backup_dir / f"{name}_globals_{timestamp}.sql.gz"
        cmd = ["pg_dumpall", *common_args, "--globals-only"]
        ok, errs = run_dump(cmd, env, out, logger)
        if ok:
            logger.info(f"[{name}] Globals dump OK → {out.name}")
        else:
            all_errors.extend([f"[{name}] globals: {e}" for e in errs])
            out.unlink(missing_ok=True)   # don't keep corrupt file

    dump_plain = server.get("dump_plain_sql", False)

    # ── Per-DB dumps ──────────────────────────────────────────────────────
    for db in server.get("databases", []):
        logger.info(f"[{name}] Dumping {db} (custom format)...")
        out = backup_dir / f"{name}_{db}_{timestamp}.dump.gz"
        cmd = [
            "pg_dump", *common_args,
            "-Fc",    # custom format: compressed, supports parallel restore
            db,
        ]
        ok, errs = run_dump(cmd, env, out, logger)
        if ok:
            logger.info(f"[{name}] {db} OK → {out.name} "
                        f"({out.stat().st_size // 1024} KB)")
        else:
            all_errors.extend([f"[{name}/{db}]: {e}" for e in errs])
            out.unlink(missing_ok=True)

        if dump_plain:
            logger.info(f"[{name}] Dumping {db} (plain SQL)...")
            sql_out = backup_dir / f"{name}_{db}_{timestamp}.sql.gz"
            sql_cmd = ["pg_dump", *common_args, "-Fp", db]
            sql_ok, sql_errs = run_dump(sql_cmd, env, sql_out, logger)
            if sql_ok:
                logger.info(f"[{name}] {db} SQL OK → {sql_out.name} "
                            f"({sql_out.stat().st_size // 1024} KB)")
            else:
                all_errors.extend(
                    [f"[{name}/{db} sql]: {e}" for e in sql_errs])
                sql_out.unlink(missing_ok=True)

    return all_errors


# ── S3 helpers ────────────────────────────────────────────────────────────────

def resolve_s3_config(block: dict, global_s3: dict, *,
                      allow_all_buckets: bool = False) -> dict:
    """Merge [s3] defaults with block overrides; validate required fields."""
    merged = {**global_s3, **block}
    all_buckets = bool(merged.get("all_buckets", False))
    bucket = merged.get("bucket")

    if allow_all_buckets and all_buckets:
        if bucket:
            raise ValueError("cannot set both bucket and all_buckets = true")
    elif not bucket:
        raise ValueError(
            "S3 block missing required field: bucket "
            "(or set all_buckets = true on [[s3_sources]])"
        )

    region = merged.get("region")
    if not region:
        raise ValueError("S3 block missing required field: region")

    endpoint_url = merged.get("endpoint_url")
    if endpoint_url and not re.match(r"^https?://", endpoint_url):
        raise ValueError(
            f"Invalid endpoint_url: {endpoint_url!r} "
            "(must be http:// or https://)"
        )

    exclude = merged.get("exclude_buckets", [])
    if exclude is not None and not isinstance(exclude, list):
        raise ValueError("exclude_buckets must be a list of bucket names")

    return {
        "bucket": bucket,
        "all_buckets": all_buckets,
        "exclude_buckets": set(exclude or []),
        "prefix": merged.get("prefix", ""),
        "region": region,
        "endpoint_url": endpoint_url,
        "access_key_id": merged.get("access_key_id"),
        "secret_access_key": merged.get("secret_access_key"),
        "addressing_style": merged.get("addressing_style"),
        "verify_ssl": merged.get("verify_ssl", True),
    }


def _s3_uri(bucket: str, prefix: str) -> str:
    prefix = prefix.lstrip("/")
    if prefix:
        return f"s3://{bucket}/{prefix}"
    return f"s3://{bucket}/"


def _build_s3_env(resolved: dict) -> dict:
    env = os.environ.copy()
    if resolved.get("access_key_id"):
        env["AWS_ACCESS_KEY_ID"] = resolved["access_key_id"]
    if resolved.get("secret_access_key"):
        env["AWS_SECRET_ACCESS_KEY"] = resolved["secret_access_key"]
    if resolved.get("region"):
        env["AWS_DEFAULT_REGION"] = resolved["region"]
    if resolved.get("addressing_style"):
        env["AWS_S3_ADDRESSING_STYLE"] = resolved["addressing_style"]
    return env


def _build_s3_sync_cmd(resolved: dict, src: str, dst: str) -> list[str]:
    cmd = ["aws"]
    if not resolved.get("verify_ssl", True):
        cmd.append("--no-verify-ssl")
    cmd += ["s3", "sync", src, dst]
    if resolved.get("region"):
        cmd += ["--region", resolved["region"]]
    if resolved.get("endpoint_url"):
        cmd += ["--endpoint-url", resolved["endpoint_url"]]
    return cmd


def _build_s3_ls_cmd(resolved: dict) -> list[str]:
    cmd = ["aws"]
    if not resolved.get("verify_ssl", True):
        cmd.append("--no-verify-ssl")
    cmd += ["s3", "ls"]
    if resolved.get("region"):
        cmd += ["--region", resolved["region"]]
    if resolved.get("endpoint_url"):
        cmd += ["--endpoint-url", resolved["endpoint_url"]]
    return cmd


def _parse_s3_ls_buckets(stdout: str) -> list[str]:
    """Parse bucket names from `aws s3 ls` output (date time bucket)."""
    buckets = []
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and re.match(r"\d{4}-\d{2}-\d{2}", parts[0]):
            buckets.append(parts[-1])
    return buckets


def list_s3_buckets(resolved: dict,
                    logger: logging.Logger) -> tuple[list[str] | None, list[str]]:
    """List bucket names visible to the configured credentials."""
    cmd = _build_s3_ls_cmd(resolved)
    env = _build_s3_env(resolved)
    ok, errs, stdout = run_aws_capture(cmd, env, logger)
    if not ok:
        return None, errs
    return _parse_s3_ls_buckets(stdout), []


def _sync_s3_bucket(resolved: dict, bucket: str, dst: Path,
                    label: str, logger: logging.Logger) -> list[str]:
    src = _s3_uri(bucket, resolved["prefix"])
    cmd = _build_s3_sync_cmd(resolved, src, str(dst))
    env = _build_s3_env(resolved)

    logger.info(f"[{label}] Syncing {src} → {dst.name}/...")
    ok, errs = run_aws_command(cmd, env, logger)
    if ok:
        logger.info(f"[{label}] S3 sync OK → {dst.name}/")
        return []
    return [f"[{label}] {e}" for e in errs]


def backup_s3_source(source: dict, global_s3: dict, backup_dir: Path,
                     timestamp: str, logger: logging.Logger) -> list[str]:
    """Pull S3 object(s) into backup_dir/{name}_{timestamp}/."""
    name = source["name"]
    try:
        resolved = resolve_s3_config(source, global_s3, allow_all_buckets=True)
    except ValueError as e:
        return [f"[{name}] {e}"]

    base_dst = backup_dir / f"{name}_{timestamp}"
    base_dst.mkdir(parents=True, exist_ok=True)

    if resolved["all_buckets"]:
        buckets, errs = list_s3_buckets(resolved, logger)
        if buckets is None:
            return [f"[{name}] {e}" for e in errs]

        exclude = resolved["exclude_buckets"]
        buckets = [b for b in buckets if b not in exclude]
        if exclude:
            logger.info(f"[{name}] Excluding bucket(s): {sorted(exclude)}")

        if not buckets:
            logger.info(f"[{name}] No buckets to sync after discovery.")
            return []

        logger.info(
            f"[{name}] Discovered {len(buckets)} bucket(s): "
            f"{', '.join(buckets)}"
        )

        all_errors: list[str] = []
        for bucket in buckets:
            dst = base_dst / bucket
            dst.mkdir(parents=True, exist_ok=True)
            all_errors.extend(
                _sync_s3_bucket(resolved, bucket, dst, f"{name}/{bucket}",
                                logger)
            )
        return all_errors

    dst = base_dst
    return _sync_s3_bucket(resolved, resolved["bucket"], dst, name, logger)


def upload_s3_destination(destination: dict, global_s3: dict,
                          backup_dir: Path,
                          logger: logging.Logger) -> list[str]:
    """Upload the full backup_dir to an S3 prefix."""
    label = "s3_destination"
    try:
        resolved = resolve_s3_config(destination, global_s3)
    except ValueError as e:
        return [f"[{label}] {e}"]

    dst = _s3_uri(resolved["bucket"], resolved["prefix"])
    cmd = _build_s3_sync_cmd(resolved, str(backup_dir), dst)
    env = _build_s3_env(resolved)

    logger.info(f"[{label}] Uploading {backup_dir} → {dst}...")
    ok, errs = run_aws_command(cmd, env, logger)
    if ok:
        logger.info(f"[{label}] S3 upload OK")
        return []
    return [f"[{label}] {e}" for e in errs]


# ── Retention ─────────────────────────────────────────────────────────────────

def prune_old_backups(backup_dir: Path, retention_days: int,
                      logger: logging.Logger):
    cutoff = datetime.now().timestamp() - retention_days * 86400
    removed_files = 0
    removed_dirs = 0
    for entry in backup_dir.glob("*"):
        if entry.stat().st_mtime >= cutoff:
            continue
        if entry.is_file():
            entry.unlink()
            logger.info(f"Pruned: {entry.name}")
            removed_files += 1
        elif entry.is_dir():
            shutil.rmtree(entry)
            logger.info(f"Pruned dir: {entry.name}")
            removed_dirs += 1
    logger.info(
        f"Pruned {removed_files} file(s) and {removed_dirs} dir(s) "
        f"older than {retention_days} days."
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    config_path = Path(os.environ.get("BACKUP_CONFIG", "backup.conf.toml"))
    if not config_path.exists():
        sys.exit(f"Config file not found: {config_path}")

    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    settings = config["settings"]
    log_file = settings.get("log_file", "/var/log/pg_backup.log")
    logger = setup_logging(log_file)

    backup_dir = Path(settings["backup_dir"])
    backup_dir.mkdir(parents=True, exist_ok=True)

    global_s3 = config.get("s3", {})
    s3_sources = config.get("s3_sources", [])
    s3_destination = config.get("s3_destination")
    servers = config.get("servers", [])

    for source in s3_sources:
        if "name" not in source:
            sys.exit("ERROR: [[s3_sources]] entry missing required field: name")
        try:
            resolve_s3_config(source, global_s3, allow_all_buckets=True)
        except ValueError as e:
            sys.exit(f"ERROR: s3_sources/{source.get('name', '?')}: {e}")

    if s3_destination:
        try:
            resolve_s3_config(s3_destination, global_s3)
        except ValueError as e:
            sys.exit(f"ERROR: s3_destination: {e}")

    if global_s3.get("endpoint_url") and (s3_sources or s3_destination):
        _validate_s3_endpoint(global_s3["endpoint_url"], logger)

    hc_url = settings.get("healthchecks_url", "")
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    logger.info("=" * 60)
    logger.info("Backup run started")
    logger.info(f"Config: {config_path}")
    if servers:
        logger.info(f"Servers: {[s['name'] for s in servers]}")
    else:
        logger.warning("No PostgreSQL servers configured; skipping DB backups.")
    if s3_sources:
        labels = []
        for s in s3_sources:
            if s.get("all_buckets"):
                labels.append(f"{s['name']} (all buckets)")
            else:
                labels.append(s["name"])
        logger.info(f"S3 sources: {labels}")
    if s3_destination:
        logger.info("S3 destination: configured")

    if hc_url:
        ping_healthchecks(hc_url, "/start", logger)

    # ── Run backups across all servers ────────────────────────────────────
    all_errors: list[str] = []

    for server in servers:
        errors = backup_server(server, backup_dir, timestamp, logger)
        all_errors.extend(errors)

    for source in s3_sources:
        errors = backup_s3_source(source, global_s3, backup_dir,
                                  timestamp, logger)
        all_errors.extend(errors)

    if s3_destination:
        errors = upload_s3_destination(s3_destination, global_s3,
                                       backup_dir, logger)
        all_errors.extend(errors)

    # ── Retention ─────────────────────────────────────────────────────────
    prune_old_backups(backup_dir, settings.get("retention_days", 14), logger)

    # ── Final status & HC ping ────────────────────────────────────────────
    if all_errors:
        logger.error("Backup run FAILED. Errors:")
        for e in all_errors:
            logger.error(f"  • {e}")
        if hc_url:
            ping_healthchecks(hc_url, "/fail", logger)
        sys.exit(1)
    else:
        logger.info("All backups completed successfully.")
        if hc_url:
            ping_healthchecks(hc_url, "", logger)   # success


if __name__ == "__main__":
    main()
