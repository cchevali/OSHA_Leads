#!/usr/bin/env python3
"""
Daily delivery entrypoint for OSHA Concierge.

Runs ingestion + email send for a customer bundle.
Writes dated run log and returns non-zero on any failure.
Sends failure notification email to admin on exception.

Features:
- Onboarding QA checks (validates customer config before sending)
- Deterministic execution (always uses repo root, logs to out/)
- Failure notifications with last successful send info
- Dry-run mode for testing without sending
"""

import argparse
import csv
import json
import os
import sqlite3
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from lead_filters import normalize_content_filter

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

# =============================================================================
# CONFIGURATION
# =============================================================================
DEFAULT_DB = "data/osha.sqlite"
OUTPUT_DIR = "out"
ADMIN_EMAIL = "support@microflowops.com"

# Required customer config fields
REQUIRED_CONFIG_FIELDS = ["customer_id", "states", "opened_window_days", "new_only_days"]


def get_script_dir() -> str:
    """Get the directory containing this script (repo root)."""
    return os.path.dirname(os.path.abspath(__file__))


def load_environment(repo_root: str) -> None:
    """Load .env for scheduler contexts that do not inherit shell variables."""
    if load_dotenv is None:
        return
    dotenv_path = Path(repo_root) / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def get_last_successful_send(log_path: str) -> dict:
    """Get last successful send from email log. Returns dict with details."""
    result = {
        "found": False,
        "summary": "No previous sends found",
        "timestamp": None,
        "recipient": None,
        "customer_id": None
    }
    
    if not os.path.exists(log_path):
        return result
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            last_success = None
            for row in reader:
                if row.get("status") == "sent":
                    last_success = row
            
            if last_success:
                result["found"] = True
                result["timestamp"] = last_success.get("timestamp", "?")
                result["recipient"] = last_success.get("recipient", "?")
                result["customer_id"] = last_success.get("customer_id", "?")
                result["summary"] = f"{result['timestamp']} to {result['recipient']}"
    except Exception as e:
        result["summary"] = f"Error reading log: {e}"
    
    return result


def send_failure_notification(error_msg: str, traceback_str: str, 
                              customer_id: str, admin_email: str,
                              email_log_path: str, run_log_path: str,
                              dry_run: bool = False) -> None:
    """Send failure notification email to admin with detailed context."""
    import smtplib
    from email.mime.text import MIMEText
    
    last_send = get_last_successful_send(email_log_path)
    
    subject = f"[OSHA Alert FAILURE] {customer_id} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = f"""OSHA Daily Delivery Failed

Customer: {customer_id}
Time: {datetime.now().isoformat()}
Run Log: {run_log_path}

Last Successful Send:
  Status: {"Found" if last_send["found"] else "None found"}
  Time: {last_send.get("timestamp", "N/A")}
  Recipient: {last_send.get("recipient", "N/A")}

Error:
{error_msg}

Traceback:
{traceback_str}

---
Action Required: Check the run log for details and resolve the issue.
This is an automated alert from OSHA Concierge.
"""
    
    if dry_run:
        print(f"[DRY-RUN] Would send failure notification to {admin_email}")
        print(f"Subject: {subject}")
        return
    
    smtp_host = os.environ.get("SMTP_HOST", "smtp.zoho.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    
    if not smtp_user or not smtp_pass:
        print(f"[ERROR] Cannot send failure notification - SMTP not configured")
        return
    
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = admin_email
        
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        
        print(f"[INFO] Failure notification sent to {admin_email}")
    except Exception as e:
        print(f"[ERROR] Failed to send failure notification: {e}")


def validate_customer_config(config: dict, config_path: str) -> list:
    """
    Validate customer configuration for onboarding QA.
    Returns list of error messages (empty if valid).
    """
    errors = []
    
    # Check required fields
    for field in REQUIRED_CONFIG_FIELDS:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    
    # Validate states
    if "states" in config:
        states = config["states"]
        if not isinstance(states, list) or len(states) == 0:
            errors.append("'states' must be a non-empty list")
        else:
            for state in states:
                if not isinstance(state, str) or len(state) != 2:
                    errors.append(f"Invalid state code: {state} (must be 2-letter string)")
    
    # Validate email_recipients
    if "email_recipients" in config:
        recipients = config["email_recipients"]
        if not isinstance(recipients, list) or len(recipients) == 0:
            errors.append("'email_recipients' must be a non-empty list")
        else:
            for r in recipients:
                if not isinstance(r, str) or "@" not in r:
                    errors.append(f"Invalid email recipient: {r}")
    else:
        errors.append("Missing 'email_recipients' field")
    
    # Validate numeric fields
    for field in ["opened_window_days", "new_only_days"]:
        if field in config:
            val = config[field]
            if not isinstance(val, int) or val <= 0:
                errors.append(f"'{field}' must be a positive integer")

    # Optional delivery controls
    if "content_filter" in config:
        try:
            normalize_content_filter(config.get("content_filter"))
        except ValueError as exc:
            errors.append(str(exc))

    if "send_time_local" in config:
        value = str(config.get("send_time_local") or "").strip()
        if len(value) != 5 or value[2] != ":":
            errors.append("'send_time_local' must be HH:MM format")

    if "timezone" in config and not str(config.get("timezone") or "").strip():
        errors.append("'timezone' cannot be blank when provided")

    if "include_low_fallback" in config and not isinstance(config.get("include_low_fallback"), bool):
        errors.append("'include_low_fallback' must be true or false")
    
    return errors


def check_suppression_enforcement(db_path: str) -> dict:
    """Verify suppression table exists and is queryable."""
    result = {"valid": False, "message": "", "count": 0}
    
    if not os.path.exists(db_path):
        result["message"] = f"Database not found: {db_path}"
        return result
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM suppression_list")
        count = cursor.fetchone()[0]
        conn.close()
        
        result["valid"] = True
        result["count"] = count
        result["message"] = f"Suppression list accessible ({count} entries)"
    except Exception as e:
        result["message"] = f"Suppression check failed: {e}"
    
    return result


def load_customer_config(config_path: str) -> dict:
    """Load customer configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def run_command(cmd: list, log_file, cwd: str) -> int:
    """Run a command and log output. Returns exit code."""
    cmd_str = " ".join(cmd)
    log_file.write(f"\n{'='*60}\n")
    log_file.write(f"Command: {cmd_str}\n")
    log_file.write(f"Working Dir: {cwd}\n")
    log_file.write(f"Time: {datetime.now().isoformat()}\n")
    log_file.write(f"{'='*60}\n\n")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd
    )
    
    log_file.write("STDOUT:\n")
    log_file.write(result.stdout or "(no output)\n")
    log_file.write("\nSTDERR:\n")
    log_file.write(result.stderr or "(no output)\n")
    log_file.write(f"\nExit Code: {result.returncode}\n")
    
    return result.returncode


def main():
    parser = argparse.ArgumentParser(
        description="Daily OSHA delivery entrypoint",
        epilog="Example: python deliver_daily.py --customer customers/sunbelt_ca_pilot.json --dry-run"
    )
    parser.add_argument("--customer", required=True, help="Path to customer config JSON")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"Path to SQLite database (default: {DEFAULT_DB})")
    parser.add_argument("--mode", choices=["baseline", "daily"], default="daily",
                        help="Delivery mode (default: daily)")
    parser.add_argument("--since-days", type=int, default=30, 
                        help="Ingestion: days to look back (default: 30)")
    parser.add_argument("--max-details", type=int, default=100,
                        help="Ingestion: max details to fetch (default: 100)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run all steps but don't send email (validates everything)")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip ingestion step")
    parser.add_argument("--admin-email", default=ADMIN_EMAIL,
                        help=f"Admin email for failure notifications (default: {ADMIN_EMAIL})")
    
    args = parser.parse_args()
    
    # ==========================================================================
    # SETUP: Ensure deterministic execution from repo root
    # ==========================================================================
    repo_root = get_script_dir()
    os.chdir(repo_root)  # Always run from repo root
    load_environment(repo_root)
    
    gen_date = datetime.now().strftime("%Y-%m-%d")
    output_dir = os.path.join(repo_root, OUTPUT_DIR)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    log_path = os.path.join(output_dir, f"run_log_{gen_date}.txt")
    email_log_path = os.path.join(output_dir, "email_log.csv")
    
    customer_id = "unknown"
    exit_code = 0
    
    try:
        # ======================================================================
        # STEP 0: Load and validate customer config (Onboarding QA)
        # ======================================================================
        print(f"[INFO] Loading customer config: {args.customer}")
        
        if not os.path.exists(args.customer):
            raise FileNotFoundError(f"Customer config not found: {args.customer}")
        
        config = load_customer_config(args.customer)
        customer_id = config.get("customer_id", "unknown")
        states = config.get("states", [])
        
        print(f"[INFO] Customer: {customer_id}")
        print(f"[INFO] Mode: {args.mode}, Dry-run: {args.dry_run}")
        print(f"[INFO] Log: {log_path}")
        
        # Validate config
        print("[INFO] Running onboarding QA checks...")
        config_errors = validate_customer_config(config, args.customer)
        if config_errors:
            for err in config_errors:
                print(f"[QA ERROR] {err}")
            raise ValueError(f"Customer config validation failed: {len(config_errors)} error(s)")
        print("[OK] Customer config valid")
        
        # Check suppression enforcement
        suppression_check = check_suppression_enforcement(args.db)
        if not suppression_check["valid"]:
            raise ValueError(f"Suppression check failed: {suppression_check['message']}")
        print(f"[OK] Suppression list accessible ({suppression_check['count']} entries)")
        
        # ======================================================================
        # STEP 1-2: Run ingestion and email delivery
        # ======================================================================
        with open(log_path, 'a', encoding='utf-8') as log_file:
            log_file.write(f"\n\n{'#'*60}\n")
            log_file.write(f"# DAILY DELIVERY RUN\n")
            log_file.write(f"# Date: {gen_date}\n")
            log_file.write(f"# Customer: {customer_id}\n")
            log_file.write(f"# Mode: {args.mode}\n")
            log_file.write(f"# Dry-run: {args.dry_run}\n")
            log_file.write(f"# Working Dir: {repo_root}\n")
            log_file.write(f"{'#'*60}\n")
            
            # Step 1: Ingestion
            if not args.skip_ingest:
                print("[INFO] Running ingestion...")
                ingest_cmd = [
                    sys.executable, "ingest_osha.py",
                    "--db", args.db,
                    "--states", ",".join(states),
                    "--since-days", str(args.since_days),
                    "--max-details", str(args.max_details)
                ]
                
                ingest_exit = run_command(ingest_cmd, log_file, repo_root)
                if ingest_exit != 0:
                    print(f"[ERROR] Ingestion failed with exit code {ingest_exit}")
                    exit_code = 1
                else:
                    print("[OK] Ingestion completed")
            else:
                log_file.write("\n[SKIPPED] Ingestion step\n")
                print("[INFO] Skipping ingestion")
            
            # Step 2: Send email
            print("[INFO] Running email delivery...")
            email_cmd = [
                sys.executable, "send_digest_email.py",
                "--db", args.db,
                "--customer", args.customer,
                "--mode", args.mode
            ]
            
            if args.dry_run:
                email_cmd.append("--dry-run")
            
            email_exit = run_command(email_cmd, log_file, repo_root)
            if email_exit != 0:
                print(f"[ERROR] Email delivery failed with exit code {email_exit}")
                exit_code = 1
            else:
                print("[OK] Email delivery completed")
            
            # ======================================================================
            # STEP 3: Verify expected outputs exist
            # ======================================================================
            if not args.dry_run:
                # Check email_log.csv was updated
                if os.path.exists(email_log_path):
                    log_file.write(f"\n[CHECK] email_log.csv exists: YES\n")
                else:
                    log_file.write(f"\n[CHECK] email_log.csv exists: NO (warning)\n")
                    print("[WARN] email_log.csv not found after run")
            
            # Summary
            status = "SUCCESS" if exit_code == 0 else "FAILURE"
            log_file.write(f"\n{'='*60}\n")
            log_file.write(f"FINAL STATUS: {status}\n")
            log_file.write(f"Completed: {datetime.now().isoformat()}\n")
            log_file.write(f"{'='*60}\n")
            
            # Send failure notification if any step failed
            if exit_code != 0:
                send_failure_notification(
                    f"Delivery pipeline returned exit code {exit_code}",
                    "See run log for details",
                    customer_id, args.admin_email,
                    email_log_path, log_path, args.dry_run
                )
    
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        
        print(f"[ERROR] Exception: {error_msg}")
        
        # Log to file
        try:
            with open(log_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"\n[EXCEPTION]\n{tb_str}\n")
        except:
            pass
        
        # Send failure notification
        send_failure_notification(
            error_msg, tb_str, customer_id, args.admin_email,
            email_log_path, log_path, args.dry_run
        )
        
        exit_code = 1
    
    # ==========================================================================
    # FINAL STATUS
    # ==========================================================================
    print("")
    if exit_code == 0:
        print(f"[SUCCESS] Daily delivery completed for {customer_id}")
    else:
        print(f"[FAILURE] Daily delivery had errors for {customer_id}")
        print(f"         Check log: {log_path}")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
