#!/usr/bin/env python3
"""
Vultr Quant Trading Server Manager
Manages Vultr instance lifecycle for quantitative trading needs.
"""

import base64
import os
import sys
import time
import logging
import argparse
from datetime import datetime

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────────────────────
API_KEY      = os.environ["VULTR_API_KEY"]
SNAPSHOT_ID  = os.environ["VULTR_SNAPSHOT_ID"]   # base snapshot to boot from
REGION       = os.environ.get("VULTR_REGION", "ewr")   # default: New Jersey (EST-friendly)
PLAN         = os.environ.get("VULTR_PLAN", "vc2-2c-4gb")
LABEL        = "Quant-Trading-Server"
TS_AUTH_KEY  = os.environ["TS_AUTH_KEY"]   # Tailscale ephemeral/reusable auth key
SNAPSHOT_RETAIN_DAYS = int(os.environ.get("VULTR_SNAPSHOT_RETAIN_DAYS", "3"))
SNAPSHOT_MAX_COUNT   = int(os.environ.get("VULTR_SNAPSHOT_MAX_COUNT", "3"))

BASE_URL = "https://api.vultr.com/v2"
HEADERS  = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

# ── Retry helper ─────────────────────────────────────────────────────────────

def _request(method: str, path: str, *, retries: int = 3, **kwargs) -> dict:
    """Thin wrapper around requests with retry + error handling.

    POST requests are never retried to avoid creating duplicate resources.
    """
    url = f"{BASE_URL}{path}"
    # POST/PATCH are not idempotent — retrying can create duplicates
    if method.upper() in ("POST", "PATCH"):
        retries = 1
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
            if resp.status_code in (200, 201, 202, 204):
                return resp.json() if resp.content else {}
            # 4xx → no point retrying
            if 400 <= resp.status_code < 500:
                log.error("Client error %s: %s", resp.status_code, resp.text)
                raise SystemExit(1)
            log.warning("Attempt %d/%d — HTTP %s: %s", attempt, retries, resp.status_code, resp.text)
        except requests.RequestException as exc:
            log.warning("Attempt %d/%d — network error: %s", attempt, retries, exc)
        if attempt < retries:
            time.sleep(10 * attempt)
    log.error("All %d attempts failed for %s %s", retries, method, path)
    raise SystemExit(1)


# ── Instance helpers ──────────────────────────────────────────────────────────

def find_instance() -> dict | None:
    """Return the running Quant-Trading-Server instance, or None."""
    data = _request("GET", "/instances")
    for inst in data.get("instances", []):
        if inst.get("label") == LABEL and inst.get("status") != "dead":
            return inst
    return None


def _build_user_data() -> str:
    """Return a Base64-encoded cloud-init shell script that joins Tailscale,
    pulls latest code, starts IBKR container, and creates a systemd service
    for the trading script."""
    script = f"""#!/bin/bash
set -euo pipefail

# 1. Join Tailscale
tailscale up --authkey={TS_AUTH_KEY} --ssh

# 2. Pull latest trading code
cd /root/algo-trading/quant && git pull --ff-only || true

# 3. Start IBKR container
cd /root/algo-trading && docker-compose up -d

# 4. Create systemd service for the trading script
cat > /etc/systemd/system/quant-trading.service <<'UNIT'
[Unit]
Description=Quant Trading Script (go.py)
After=docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=/root/algo-trading/quant
ExecStartPre=/bin/sleep 10
ExecStart=/usr/bin/python3 go.py
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now quant-trading.service
"""
    return base64.b64encode(script.encode()).decode()


def latest_backup_snapshot() -> str | None:
    """Return the ID of the most recent Quant-Backup-* snapshot, or None."""
    data = _request("GET", "/snapshots")
    backups = [
        s for s in data.get("snapshots", [])
        if s.get("description", "").startswith("Quant-Backup-") and s.get("status") == "complete"
    ]
    if not backups:
        return None
    backups.sort(key=lambda s: s["description"], reverse=True)
    snap = backups[0]
    log.info("Latest backup snapshot: %s (%s)", snap["id"], snap["description"])
    return snap["id"]


def create_instance() -> dict:
    """Create a new instance, preferring the latest daily backup over the base snapshot."""
    snap_id = latest_backup_snapshot() or SNAPSHOT_ID
    log.info("Creating instance from snapshot %s in region %s …", snap_id, REGION)
    payload = {
        "region":         REGION,
        "plan":           PLAN,
        "snapshot_id":    snap_id,
        "label":          LABEL,
        "backups":        "disabled",
        "user_data":      _build_user_data(),  # auto-join Tailscale on first boot
    }
    data = _request("POST", "/instances", json=payload)
    instance = data["instance"]
    log.info("Instance %s created — waiting for it to become active …", instance["id"])
    return _wait_for_instance(instance["id"])


def _wait_for_instance(instance_id: str, timeout: int = 600) -> dict:
    """Poll until the instance status is 'active'."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = _request("GET", f"/instances/{instance_id}")
        inst = data["instance"]
        log.info("  Instance status: %s  power: %s", inst["status"], inst["power_status"])
        if inst["status"] == "active" and inst["power_status"] == "running":
            log.info("Instance is live. Main IP: %s", inst["main_ip"])
            return inst
        time.sleep(20)
    log.error("Instance did not become active within %ds", timeout)
    raise SystemExit(1)


def destroy_instance(instance_id: str) -> None:
    log.info("Destroying instance %s …", instance_id)
    _request("DELETE", f"/instances/{instance_id}")
    log.info("Instance %s destroyed.", instance_id)


# ── Snapshot helpers ──────────────────────────────────────────────────────────

def create_snapshot(instance_id: str) -> str:
    """Create a daily snapshot and return its ID."""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    desc = f"Quant-Backup-{date_str}"
    log.info("Creating snapshot '%s' for instance %s …", desc, instance_id)
    data = _request("POST", "/snapshots", json={"instance_id": instance_id, "description": desc})
    snap_id = data["snapshot"]["id"]
    log.info("Snapshot %s queued.", snap_id)
    return snap_id


def wait_for_snapshot(snap_id: str, timeout: int = 3600) -> None:
    """Poll until the snapshot status is 'complete'."""
    log.info("Waiting for snapshot %s to complete …", snap_id)
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = _request("GET", f"/snapshots/{snap_id}")
        status = data["snapshot"]["status"]
        log.info("  Snapshot status: %s", status)
        if status == "complete":
            log.info("Snapshot %s is complete.", snap_id)
            return
        if status == "error":
            log.error("Snapshot %s entered error state.", snap_id)
            raise SystemExit(1)
        time.sleep(30)
    log.error("Snapshot %s did not complete within %ds", snap_id, timeout)
    raise SystemExit(1)


def prune_old_snapshots(
    retain_days: int = SNAPSHOT_RETAIN_DAYS,
    max_count: int = SNAPSHOT_MAX_COUNT,
) -> None:
    """Delete Quant-Backup snapshots that are older than `retain_days` OR exceed `max_count`."""
    data = _request("GET", "/snapshots")
    backups = [
        s for s in data.get("snapshots", [])
        if s.get("description", "").startswith("Quant-Backup-")
    ]
    if not backups:
        log.info("No backup snapshots found.")
        return

    # Sort newest first by date embedded in description (YYYYMMDD)
    backups.sort(key=lambda s: s["description"], reverse=True)

    to_delete = set()

    # Rule 1: delete snapshots older than retain_days (but always keep the newest one)
    for snap in backups[1:]:  # skip the newest — never delete it
        date_str = snap["description"].replace("Quant-Backup-", "")
        try:
            snap_date = datetime.strptime(date_str, "%Y%m%d")
            age_days = (datetime.utcnow() - snap_date).days
            if age_days > retain_days:
                to_delete.add(snap["id"])
        except ValueError:
            pass  # skip snapshots with unexpected description format

    # Rule 2: keep at most max_count snapshots (delete oldest surplus)
    for snap in backups[max_count:]:
        to_delete.add(snap["id"])

    if not to_delete:
        log.info("No old snapshots to prune (retain_days=%d, max_count=%d).", retain_days, max_count)
        return

    for snap in backups:
        if snap["id"] in to_delete:
            log.info("Pruning snapshot %s (%s) …", snap["id"], snap["description"])
            _request("DELETE", f"/snapshots/{snap['id']}")
    log.info("Pruned %d snapshot(s).", len(to_delete))


# ── High-level actions ────────────────────────────────────────────────────────

def action_start() -> None:
    existing = find_instance()
    if existing:
        log.info("Instance already running: %s (%s). Nothing to do.", existing["id"], existing["main_ip"])
        return
    inst = create_instance()
    log.info("Server started successfully. ID=%s  IP=%s", inst["id"], inst["main_ip"])


def action_stop() -> None:
    inst = find_instance()
    if not inst:
        log.error("No running '%s' instance found.", LABEL)
        raise SystemExit(1)

    instance_id = inst["id"]

    # 1. Snapshot
    snap_id = create_snapshot(instance_id)

    # 2. Wait for snapshot to complete (critical — never destroy before this)
    wait_for_snapshot(snap_id)

    # 3. Destroy instance
    destroy_instance(instance_id)

    # 4. Prune old snapshots (optional, best-effort)
    try:
        prune_old_snapshots()
    except Exception as exc:
        log.warning("Snapshot pruning failed (non-fatal): %s", exc)

    log.info("Stop sequence complete.")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Vultr Quant Server Manager")
    parser.add_argument("action", choices=["start", "stop"], help="Lifecycle action")
    args = parser.parse_args()

    if args.action == "start":
        action_start()
    else:
        action_stop()


if __name__ == "__main__":
    main()
