"""
production_config.py — corrected

Goals
- Centralize all config (paths, tz, thresholds, holidays, emails, services)
- Load from environment / .env.production (no secrets in code)
- Provide safe defaults for local dev; strict for production
- Expose a single SETTINGS dict used by the rest of the app
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List

# Optional .env support (python-dotenv)
try:  # pragma: no cover
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.getenv("ENV_FILE", ".env.production"), override=False)
except Exception:
    pass

# ------------- Helpers ------------- #
def _get(name: str, default: str | None = None, required: bool = False) -> str | None:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def _split_csv(val: str | None) -> List[str]:
    return [x.strip() for x in (val or "").split(",") if x.strip()]


# ------------- Core settings ------------- #
TIMEZONE = _get("APP_TIMEZONE", "Africa/Lagos")

PATHS = {
    "data_dir": _get("APP_DATA_DIR", "./data"),
    "processed_dir": _get("APP_PROCESSED_DIR", "./data/processed"),
    "reports_dir": _get("APP_REPORTS_DIR", "./reports"),
    "logs_dir": _get("APP_LOGS_DIR", "./logs"),
}

THRESHOLDS = {
    # FX
    "fx_markup_tolerance_pct": float(_get("FX_MARKUP_TOL_PCT", "2.5")),
    "fx_inr_tolerance": float(_get("FX_INR_TOL", "100")),
    # Matching
    "amount_match_tolerance_inr": float(_get("AMOUNT_MATCH_TOL_INR", "10")),
    "date_window_days": int(_get("DATE_WINDOW_DAYS", "2")),
    # CC SLA
    "late_entry_working_days": int(_get("LATE_ENTRY_WORKING_DAYS", "3")),
    "invoice_later_days": int(_get("INVOICE_LATER_DAYS", "14")),
    "finance_escalation_days": int(_get("FINANCE_ESCALATION_DAYS", "30")),
}

# ISO date list (comma-separated): 2025-01-01,2025-08-15
HOLIDAYS = _split_csv(_get("APP_HOLIDAYS", ""))

EMAIL = {
    "from": _get("SMTP_FROM", "noreply@yourdomain.com"),
    "ap_team": _split_csv(_get("AP_TEAM", "ap@yourdomain.com")),
    "finance_mgmt": _split_csv(_get("FINANCE_MGMT", "finance@yourdomain.com")),
}

# ------------- Services ------------- #
DATABASE = {
    "url": _get("DATABASE_URL", "postgresql+psycopg2://reconciliation_user:secure_production_password@db:5432/reconciliation_prod"),
}

REDIS = {"url": _get("REDIS_URL", "redis://redis:6379/0")}

SMTP = {
    "host": _get("SMTP_HOST", "smtp.office365.com"),
    "port": int(_get("SMTP_PORT", "587")),
    "user": _get("SMTP_USER", None),
    "pass": _get("SMTP_PASS", None),
    "use_tls": _get("SMTP_USE_TLS", "true").lower() == "true",
}

SLACK = {
    "bot_token": _get("SLACK_BOT_TOKEN"),
    "default_channel": _get("SLACK_DEFAULT_CHANNEL", "#recon-alerts"),
}

STRIPE = {
    "api_key": _get("STRIPE_API_KEY", required=True),
}

FX = {
    "provider": _get("FX_PROVIDER", "fixer"),  # fixer|ecb|none
    "api_key": _get("FX_API_KEY"),
}

RMS = {
    "base_url": _get("RMS_BASE_URL", "https://rms.koenig-solutions.com"),
    "username": _get("RMS_USER"),
    "password": _get("RMS_PASS"),
}

# ------------- Final exported dict ------------- #
SETTINGS: Dict[str, Any] = {
    "timezone": TIMEZONE,
    "paths": PATHS,
    "thresholds": THRESHOLDS,
    "holidays": HOLIDAYS,  # a list of ISO strings; convert to dates where needed
    "email": EMAIL,
    "database": DATABASE,
    "redis": REDIS,
    "smtp": SMTP,
    "slack": SLACK,
    "stripe": STRIPE,
    "fx": FX,
    "rms": RMS,
}

if __name__ == "__main__":  # quick sanity print
    import json
    # Never print secrets; mask the sensitive ones
    safe = dict(SETTINGS)
    safe["smtp"] = {**SMTP, "user": "***" if SMTP.get("user") else None, "pass": "***" if SMTP.get("pass") else None}
    safe["stripe"] = {"api_key": "***" if STRIPE.get("api_key") else None}
    safe["fx"] = {**FX, "api_key": "***" if FX.get("api_key") else None}
    safe["rms"] = {**RMS, "username": "***" if RMS.get("username") else None, "password": "***" if RMS.get("password") else None}
    print(json.dumps(safe, indent=2))
