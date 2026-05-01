#!/usr/bin/env python3
"""
pg_backup.py — multi-server Postgres backup with Healthchecks.io monitoring.
Single HC check: only pings success if ALL dumps succeed.
"""

import gzip
import json
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
    logger = logging.getLogger("pg_backup")
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

    hc_url = settings.get("healthchecks_url", "")
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    logger.info("=" * 60)
    logger.info("Backup run started")
    logger.info(f"Config: {config_path}")
    logger.info(f"Servers: {[s['name'] for s in config['servers']]}")

    if hc_url:
        ping_healthchecks(hc_url, "/start", logger)

    # ── Run backups across all servers ────────────────────────────────────
    all_errors: list[str] = []

    for server in config["servers"]:
        errors = backup_server(server, backup_dir, timestamp, logger)
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