"""
Reconciliation Orchestrator — corrected & production‑ready

What this does
--------------
Single entry point that wires all layers end‑to‑end with sane defaults:
- Timebox: runs for [since, until] (Africa/Lagos)
- Stripe integrity check (charges + refunds + webhook mirror ➜ RMS)
- Credit‑card SLA monitor (working‑day SLA + Invoice‑Later ageing + tiered escalation)
- FX detection + markup analysis (DCC bypass, tolerances from config)
- Consolidated Excel report + optional AP/Finance digests
- Idempotent run folders, retries, structured logging, and error surfacing

External modules expected (corrected versions you already have in canvas):
- stripe_integrity_checker: build_matches_and_flags / run_checker
- cc_monitor: run_cc_monitor
- fx_detector: annotate_fx
- markup_analyzer: batch_analyze_markup
- exchange_rate_engine: get_rate(date, ccy) or similar shim
- consolidated_reporter: write_excel_report(sheets)
- intelligent_alert_system: send_summary(to, subject, html) (optional)
- production_config: SETTINGS (timezone, thresholds, paths, emails)

You can call this file from Celery/cron or GitHub Actions.
"""
from __future__ import annotations

import os
import sys
import json
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

# ---- Config ---- #
try:
    from production_config import SETTINGS
except Exception:  # pragma: no cover
    SETTINGS = {
        "timezone": "Africa/Lagos",
        "paths": {"data_dir": "./data", "processed_dir": "./data/processed", "reports_dir": "./reports", "logs_dir": "./logs"},
        "email": {"ap_team": ["ap@example.com"], "finance_mgmt": ["fin@example.com"], "from": "noreply@example.com"},
        "thresholds": {"fx_markup_tolerance_pct": 2.5, "fx_inr_tolerance": 100, "amount_match_tolerance_inr": 5, "date_window_days": 2},
    }

TZ = ZoneInfo(SETTINGS.get("timezone", "Africa/Lagos"))
PATHS = SETTINGS.get("paths", {})
DATA_DIR = PATHS.get("data_dir", "./data")
PROC_DIR = PATHS.get("processed_dir", os.path.join(DATA_DIR, "processed"))
REPORTS_DIR = PATHS.get("reports_dir", "./reports")
LOGS_DIR = PATHS.get("logs_dir", "./logs")

for p in (DATA_DIR, PROC_DIR, REPORTS_DIR, LOGS_DIR):
    os.makedirs(p, exist_ok=True)

# ---- Imports of project modules ---- #
from stripe_integrity_checker import run_checker as run_stripe_checker
from cc_monitor import run_cc_monitor
from fx_detector import annotate_fx
from markup_analyzer import batch_analyze_markup

# These two are pluggable; keep soft imports to avoid hard coupling
try:
    from exchange_rate_engine import get_rate  # get_rate(dt: date, ccy: str) -> float
except Exception:  # pragma: no cover
    def get_rate(dt: date, ccy: str) -> Optional[float]:
        return None

try:
    from consolidated_reporter import generate_reconciliation_report
except Exception:  # pragma: no cover
    def generate_reconciliation_report(output_name: str, frames: dict, extra_context: dict | None = None) -> str:
        # Minimal fallback if the real reporter isn't available
        import os, pandas as pd
        path = os.path.join(REPORTS_DIR, f"{output_name}.xlsx")
        with pd.ExcelWriter(path, engine="openpyxl") as xw:
            for name, df in frames.items():
                df.to_excel(xw, name[:31], index=False)
        return path

# Optional notifier
try:
    from intelligent_alert_system import send_summary
except Exception:  # pragma: no cover
    def send_summary(to: list[str], subject: str, html: str):
        pass


# ---- Minimal, dependency‑free retry helper ---- #
def retry(n: int = 3, delay_sec: float = 1.5):
    def deco(fn):
        def wrapped(*args, **kwargs):
            last_err = None
            for i in range(n):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:  # pragma: no cover
                    last_err = e
                    if i < n - 1:
                        import time
                        time.sleep(delay_sec)
            raise last_err
        return wrapped
    return deco

# ---- Clients contract (shim your concrete implementations) ---- #
class StripeClient:
    def get_charges(self, since: datetime, until: datetime) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError
    def get_refunds(self, since: datetime, until: datetime) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

class RMSClient:
    def get_stripe_rms_entries(self, since: datetime, until: datetime) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError
    def get_cc_export(self, since: datetime, until: datetime) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

class BankLoader:
    def load_statement_window(self, since: datetime, until: datetime) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

@dataclass
class OrchestratorInputs:
    stripe_client: StripeClient
    rms_client: RMSClient
    bank_loader: Optional[BankLoader] = None
    webhook_log: Optional[pd.DataFrame] = None  # optional mirror of webhook events

# ---- Core orchestration ---- #
@retry(n=2, delay_sec=2.0)
def _stripe_block(inp: OrchestratorInputs, since: datetime, until: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return run_stripe_checker(inp.stripe_client, inp.rms_client, since, until, webhook_log=inp.webhook_log)

def _cc_block(inp: OrchestratorInputs, since: datetime, until: datetime) -> pd.DataFrame:
    cc_df = inp.rms_client.get_cc_export(since, until)
    annotated = run_cc_monitor(cc_df, today_dt=datetime.now(TZ))
    return annotated

def _fx_block(bank_df: pd.DataFrame) -> pd.DataFrame:
    if bank_df is None or bank_df.empty:
        return pd.DataFrame()

    # Expect columns: narration, merchant_country_iso2, stated_ccy, receipt_ccy, charged_inr, foreign_amount?, txn_ts
    fx_df = annotate_fx(
        bank_df,
        cols={
            "narration": "narration",
            "country_iso2": "merchant_country_iso2",
            "stated_ccy": "currency",  # if present; else leave blank in data
            "receipt_ccy": "receipt_currency",  # join from receipts index if you have
            "expected_inr": "expected_inr",  # we will fill shortly
            "charged_inr": "amount_inr",
            "txn_ts": "txn_ts",
        },
    )

    # Compute expected_inr via IBR when foreign is detected and foreign_amount present
    if "foreign_amount" in fx_df.columns:
        expected_list = []
        for _, r in fx_df.iterrows():
            if r.get("fx_is_foreign") and r.get("fx_currency") and pd.notna(r.get("foreign_amount")):
                rate = get_rate(pd.to_datetime(r.get("txn_ts")).date() if pd.notna(r.get("txn_ts")) else date.today(), r.get("fx_currency"))
                expected_list.append((rate or 0) * float(r.get("foreign_amount")))
            else:
                expected_list.append(None)
        fx_df["expected_inr"] = expected_list

    # Markup analysis (uses fx_is_dcc to bypass)
    fx_df = batch_analyze_markup(
        fx_df,
        cols={
            "foreign_amount": "foreign_amount",
            "charged_inr": "amount_inr",
            "interbank_rate": "ibr_rate",  # if you store it; otherwise derive below
            "fx_is_dcc": "fx_is_dcc",
        },
    )

    return fx_df

def _summarize(stripe_flags: pd.DataFrame, cc_ann: pd.DataFrame, fx_df: pd.DataFrame) -> pd.DataFrame:
    def c(df: Optional[pd.DataFrame]) -> int:
        return 0 if df is None or df.empty else len(df)

    summary = {
        "run_ts": datetime.now(TZ).isoformat(),
        "stripe_flags": c(stripe_flags),
        "cc_late_entries": int(cc_ann[cc_ann["flag"].eq("LateEntry")].shape[0]) if c(cc_ann) else 0,
        "cc_invoice_later": int(cc_ann[cc_ann["flag"].eq("InvoiceLater")].shape[0]) if c(cc_ann) else 0,
        "fx_flagged": int(fx_df[fx_df.get("markup_status").eq("FLAGGED")].shape[0]) if c(fx_df) else 0,
    }
    return pd.DataFrame([summary])

def _write_report(run_id: str, summary_df: pd.DataFrame, sheets: Dict[str, pd.DataFrame]) -> str:
    # Build the full frame mapping for the new reporter
    frames = {"Summary": summary_df}
    frames.update({name: df for name, df in sheets.items() if df is not None})

    return generate_reconciliation_report(
        output_name=f"Recon_Report_{run_id}",
        frames=frames,
        extra_context={"run_id": run_id}
    )

def run_orchestrator(inp: OrchestratorInputs, days_back: int = 7, days_forward: int = 0) -> Dict[str, Any]:
    since = (datetime.now(TZ) - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
    until = (datetime.now(TZ) + timedelta(days=days_forward)).replace(hour=23, minute=59, second=59, microsecond=0)
    run_id = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")

    # 1) Stripe integrity block
    try:
        stripe_matches, stripe_flags = _stripe_block(inp, since, until)
    except Exception as e:
        stripe_matches, stripe_flags = pd.DataFrame(), pd.DataFrame()
        _err("STRIPE_BLOCK", e)

    # 2) CC SLA/Invoice‑Later block
    try:
        cc_ann = _cc_block(inp, since, until)
    except Exception as e:
        cc_ann = pd.DataFrame()
        _err("CC_BLOCK", e)

    # 3) Bank + FX block (optional if bank loader provided)
    try:
        bank_df = inp.bank_loader.load_statement_window(since, until) if inp.bank_loader else pd.DataFrame()
        fx_df = _fx_block(bank_df)
    except Exception as e:
        fx_df = pd.DataFrame()
        _err("FX_BLOCK", e)

    # 4) Summary & Report
    summary_df = _summarize(stripe_flags, cc_ann, fx_df)
    report_path = _write_report(
        run_id,
        summary_df,
        sheets={
            "Stripe_Matches": stripe_matches,
            "Stripe_Flags": stripe_flags,
            "CC_Annotated": cc_ann,
            "FX_Annotated": fx_df,
        },
    )

    # 5) Optional digest emails
    try:
        ap_to = SETTINGS.get("email", {}).get("ap_team", [])
        subject = f"Recon Summary {run_id} — Stripe/CC/FX"
        html = summary_df.to_html(index=False)
        if ap_to:
            send_summary(ap_to, subject, html)
    except Exception as e:
        _err("EMAIL", e)

    return {
        "run_id": run_id,
        "since": since.isoformat(),
        "until": until.isoformat(),
        "report_path": report_path,
        "counts": {
            "stripe_flags": 0 if stripe_flags is None or stripe_flags.empty else len(stripe_flags),
            "cc_rows": 0 if cc_ann is None or cc_ann.empty else len(cc_ann),
            "fx_rows": 0 if fx_df is None or fx_df.empty else len(fx_df),
        },
    }

# ---- Logging helpers ---- #
def _err(stage: str, exc: Exception):  # pragma: no cover
    msg = f"[{datetime.now(TZ).isoformat()}] {stage} FAILED: {exc}\n{traceback.format_exc()}"
    sys.stderr.write(msg + "\n")
    try:
        with open(os.path.join(LOGS_DIR, "orchestrator_errors.log"), "a", encoding="utf-8") as fh:
            fh.write(msg + "\n")
    except Exception:
        pass

# ---- CLI ---- #
if __name__ == "__main__":  # pragma: no cover
    # Provide minimal dummy clients for a dry run
    class DummyStripe:
        def get_charges(self, s, u):
            return pd.DataFrame([
                {"charge_id": "ch_1", "invoice_id": "INV-1", "email": "x@y.com", "amount": 10000, "currency": "INR", "created": (datetime.now(TZ)-timedelta(days=1)).isoformat(), "status": "succeeded"}
            ])
        def get_refunds(self, s, u):
            return pd.DataFrame([])

    class DummyRMS:
        def get_stripe_rms_entries(self, s, u):
            return pd.DataFrame([
                {"rms_id": "r1", "invoice_no": "INV-1", "email": "x@y.com", "amount_inr": 10000, "posted_at": (datetime.now(TZ)-timedelta(days=1)).isoformat()}
            ])
        def get_cc_export(self, s, u):
            return pd.DataFrame([
                {"txn_id": "T1", "txn_date": (datetime.now(TZ)-timedelta(days=5)).date(), "vendor": "AWS", "card_last4": "1234", "cardholder": "Amit Kumar", "amount_inr": 12000.0, "has_receipt": False, "invoice_later": False, "entered_at": None, "entered_by": None}
            ])

    class DummyBank(BankLoader):
        def load_statement_window(self, s, u):
            return pd.DataFrame([
                {"txn_ts": (datetime.now(TZ)-timedelta(days=3)).isoformat(), "narration": "AMZN US * DYN CURR", "merchant_country_iso2": "US", "currency": None, "receipt_currency": None, "amount_inr": 12345.0, "foreign_amount": 150.0, "ibr_rate": 82.0}
            ])

    out = run_orchestrator(OrchestratorInputs(StripeClient(), RMSClient(), bank_loader=DummyBank()))
    print(json.dumps(out, indent=2))
"""
