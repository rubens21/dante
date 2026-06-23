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
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

# Python 3.11+: tomllib is stdlib. For 3.10 and below, install tomli:
#   pip install tomli
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        sys.exit("ERROR: Python < 3.11 detected. Run: pip install tomli")


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
    errors = []

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            env=env,
            text=True,
        )

        if proc.stdout:
            for line in proc.stdout.splitlines():
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

    return (len(errors) == 0), errors


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

def resolve_s3_config(block: dict, global_s3: dict) -> dict:
    """Merge [s3] defaults with block overrides; validate required fields."""
    merged = {**global_s3, **block}

    bucket = merged.get("bucket")
    if not bucket:
        raise ValueError("S3 block missing required field: bucket")

    region = merged.get("region")
    if not region:
        raise ValueError("S3 block missing required field: region")

    endpoint_url = merged.get("endpoint_url")
    if endpoint_url and not re.match(r"^https?://", endpoint_url):
        raise ValueError(
            f"Invalid endpoint_url: {endpoint_url!r} "
            "(must be http:// or https://)"
        )

    return {
        "bucket": bucket,
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


def backup_s3_source(source: dict, global_s3: dict, backup_dir: Path,
                     timestamp: str, logger: logging.Logger) -> list[str]:
    """Pull an S3 prefix into backup_dir/{name}_{timestamp}/."""
    name = source["name"]
    try:
        resolved = resolve_s3_config(source, global_s3)
    except ValueError as e:
        return [f"[{name}] {e}"]

    dst = backup_dir / f"{name}_{timestamp}"
    dst.mkdir(parents=True, exist_ok=True)

    src = _s3_uri(resolved["bucket"], resolved["prefix"])
    cmd = _build_s3_sync_cmd(resolved, src, str(dst))
    env = _build_s3_env(resolved)

    logger.info(f"[{name}] Syncing {src} → {dst.name}/...")
    ok, errs = run_aws_command(cmd, env, logger)
    if ok:
        logger.info(f"[{name}] S3 sync OK → {dst.name}/")
        return []
    return [f"[{name}] {e}" for e in errs]


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
    removed = 0
    for f in backup_dir.glob("*"):
        if f.is_file() and f.stat().st_mtime < cutoff:
            f.unlink()
            logger.info(f"Pruned: {f.name}")
            removed += 1
    logger.info(f"Pruned {removed} file(s) older than {retention_days} days.")


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

    for source in s3_sources:
        if "name" not in source:
            sys.exit("ERROR: [[s3_sources]] entry missing required field: name")
        try:
            resolve_s3_config(source, global_s3)
        except ValueError as e:
            sys.exit(f"ERROR: s3_sources/{source.get('name', '?')}: {e}")

    if s3_destination:
        try:
            resolve_s3_config(s3_destination, global_s3)
        except ValueError as e:
            sys.exit(f"ERROR: s3_destination: {e}")

    hc_url = settings.get("healthchecks_url", "")
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    logger.info("=" * 60)
    logger.info("Backup run started")
    logger.info(f"Config: {config_path}")
    logger.info(f"Servers: {[s['name'] for s in config['servers']]}")
    if s3_sources:
        logger.info(f"S3 sources: {[s['name'] for s in s3_sources]}")
    if s3_destination:
        logger.info("S3 destination: configured")

    if hc_url:
        ping_healthchecks(hc_url, "/start", logger)

    # ── Run backups across all servers ────────────────────────────────────
    all_errors: list[str] = []

    for server in config["servers"]:
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
