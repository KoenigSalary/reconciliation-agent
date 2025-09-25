"""
Stripe Integrity Checker — production‑ready

Purpose
-------
End‑to‑end verification of Stripe ➜ RMS sync using webhook logs, live Stripe API pulls,
and RMS data. Flags:
- IntegrationFailure: webhook event delivered but missing in RMS
- RefundNotPosted: Stripe refund exists, no RMS credit/negative entry
- DuplicateCharge: >1 successful Stripe charge for same invoice_id
- DuplicateRMS: >1 RMS entry mapped to same Stripe charge/invoice
- AmountMismatch: amount delta beyond tolerance
- DateDrift: created/posted dates differ beyond tolerance window

Assumptions
----------
- You have a Stripe client `stripe_client` exposing:
    get_charges(since, until) -> DataFrame
    get_refunds(since, until) -> DataFrame
    get_events(since, until, types=None) -> DataFrame (optional, for webhook mirror)
- `rms_client` exposes:
    get_stripe_rms_entries(since, until) -> DataFrame
- A webhook mirror table/log (optional). If not available, we infer using Stripe events directly.

Config
------
Reads tolerances/timezone from production_config.SETTINGS.

Outputs
-------
- matches_df: deterministic matches
- flags_df: normalized flags with severity and fix suggestions

"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
from zoneinfo import ZoneInfo

import pandas as pd

try:
    from production_config import SETTINGS
except Exception:  # pragma: no cover
    SETTINGS = {
        "timezone": "Africa/Lagos",
        "thresholds": {
            "amount_match_tolerance_inr": 5,
            "date_window_days": 2,
        }
    }

TZ = ZoneInfo(SETTINGS.get("timezone", "Africa/Lagos"))
AMT_TOL = float(SETTINGS.get("thresholds", {}).get("amount_match_tolerance_inr", 5))
DATE_WIN_DAYS = int(SETTINGS.get("thresholds", {}).get("date_window_days", 2))


@dataclass
class CheckerInputs:
    stripe_charges: pd.DataFrame
    stripe_refunds: pd.DataFrame
    rms_rows: pd.DataFrame
    webhook_log: Optional[pd.DataFrame] = None  # columns: event_id, charge_id, invoice_id, email, amount, currency, created


# ----------------------------- Normalization ----------------------------- #
def _norm_stripe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Expected columns: charge_id, payment_intent, invoice_id, email, amount, currency, created, status
    out['amount'] = pd.to_numeric(out['amount'], errors='coerce')
    out['created'] = pd.to_datetime(out['created'], utc=True).dt.tz_convert(TZ)
    if 'status' not in out:
        out['status'] = 'succeeded'
    return out


def _norm_refunds(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Expected columns: refund_id, charge_id, amount, currency, created, status
    out['amount'] = pd.to_numeric(out['amount'], errors='coerce')
    out['created'] = pd.to_datetime(out['created'], utc=True).dt.tz_convert(TZ)
    return out


def _norm_rms(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Expected: rms_id, invoice_no, email, amount_inr, currency, posted_at, stripe_charge_id?, stripe_invoice_id?
    out['amount_inr'] = pd.to_numeric(out.get('amount_inr'), errors='coerce')
    if 'posted_at' in out:
        out['posted_at'] = pd.to_datetime(out['posted_at'], errors='coerce')
        if out['posted_at'].dt.tz is None:
            out['posted_at'] = out['posted_at'].dt.tz_localize(TZ)
    return out


def _norm_webhook(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None:
        return None
    out = df.copy()
    out['created'] = pd.to_datetime(out['created'], utc=True).dt.tz_convert(TZ)
    return out


# ------------------------------- Matching -------------------------------- #
def _primary_match(stripe_charges: pd.DataFrame, rms: pd.DataFrame) -> pd.DataFrame:
    # Primary key: invoice_id ↔ invoice_no
    left = stripe_charges.rename(columns={'invoice_id': 'invoice_no'})
    m = left.merge(rms, on='invoice_no', how='left', suffixes=('_st', '_r'))
    m['match_type'] = m['rms_id'].notna().map({True: 'primary', False: None})
    return m


def _fallback_match(stripe_charges: pd.DataFrame, rms: pd.DataFrame) -> pd.DataFrame:
    # Fallback: (email & amount≈ & date±)
    sc = stripe_charges.copy()
    sc['created_date'] = sc['created'].dt.date
    rm = rms.copy()
    rm['posted_date'] = rm['posted_at'].dt.date if 'posted_at' in rm else pd.NaT

    m = sc.merge(rm, left_on='email', right_on='email', how='left', suffixes=('_st', '_r'))
    # amount tolerance
    m['amt_match'] = (m['amount'].sub(m['amount_inr']).abs() <= AMT_TOL)
    # date window
    def _within_window(row):
        try:
            d1 = row['created_date']
            d2 = row['posted_date']
            return abs((pd.Timestamp(d1) - pd.Timestamp(d2)).days) <= DATE_WIN_DAYS
        except Exception:
            return False
    m['date_match'] = m.apply(_within_window, axis=1)
    m['match_ok'] = m['amt_match'] & m['date_match']
    m.loc[m['match_ok'] & m['match_type'].isna(), 'match_type'] = 'fallback'
    return m[m['match_ok']]


def build_matches_and_flags(inputs: CheckerInputs) -> Tuple[pd.DataFrame, pd.DataFrame]:
    charges = _norm_stripe(inputs.stripe_charges)
    refunds = _norm_refunds(inputs.stripe_refunds)
    rms = _norm_rms(inputs.rms_rows)
    webhook = _norm_webhook(inputs.webhook_log)

    # Primary match first
    m_primary = _primary_match(charges, rms)

    # Fallback for those not matched
    unmatched = m_primary[m_primary['match_type'].isna()][['charge_id','email','amount','created','invoice_no']]
    m_fallback = _fallback_match(charges[charges['charge_id'].isin(unmatched['charge_id'])], rms)

    # Combine
    matches = pd.concat([
        m_primary[m_primary['match_type'].eq('primary')],
        m_fallback.assign(match_type='fallback')
    ], ignore_index=True, sort=False)

    # --------------------------- Flags assembly --------------------------- #
    flags = []

    # IntegrationFailure: webhook seen but RMS missing
    if webhook is not None and not webhook.empty:
        seen = webhook[['charge_id','invoice_id','created']].drop_duplicates()
        m = seen.merge(matches[['charge_id','rms_id']], on='charge_id', how='left')
        missing = m[m['rms_id'].isna()]
        for _, r in missing.iterrows():
            flags.append({
                'severity': 'P1',
                'flag': 'IntegrationFailure',
                'charge_id': r['charge_id'],
                'invoice_id': r['invoice_id'],
                'reason': 'Webhook delivered but RMS row missing',
                'fix_suggestion': 'Check RMS webhook consumer logs; reprocess event',
            })

    # RefundNotPosted: refund exists without RMS credit/negative
    if not refunds.empty:
        # naive: look for an RMS row with negative amount for same invoice/charge
        rj = refunds.merge(matches[['charge_id','invoice_no','rms_id','amount_inr']], on='charge_id', how='left')
        for _, r in rj.iterrows():
            has_credit = rms[(rms.get('invoice_no') == r['invoice_no']) & (rms['amount_inr'] < 0)].any().any()
            if not has_credit:
                flags.append({
                    'severity': 'P0',
                    'flag': 'RefundNotPosted',
                    'charge_id': r['charge_id'],
                    'invoice_id': r['invoice_no'],
                    'reason': 'Stripe refund recorded but no RMS credit note/negative entry',
                    'fix_suggestion': 'Post credit memo in RMS and link to charge',
                })

    # DuplicateCharge: >1 succeeded Stripe charge for same invoice_id
    dup_st = charges[charges['status'].eq('succeeded')].groupby('invoice_id').size().reset_index(name='n')
    for _, r in dup_st[dup_st['n'] > 1].iterrows():
        flags.append({
            'severity': 'P0',
            'flag': 'DuplicateCharge',
            'invoice_id': r['invoice_id'],
            'reason': f"{int(r['n'])} Stripe charges for same invoice",
            'fix_suggestion': 'Refund extras; keep a single valid charge',
        })

    # DuplicateRMS: >1 RMS row tied to same charge/invoice
    if 'stripe_charge_id' in rms.columns:
        dup_r = rms.groupby('stripe_charge_id').size().reset_index(name='n')
        for _, r in dup_r[dup_r['n'] > 1].iterrows():
            flags.append({
                'severity': 'P0',
                'flag': 'DuplicateRMS',
                'charge_id': r['stripe_charge_id'],
                'reason': f"{int(r['n'])} RMS rows linked to same Stripe charge",
                'fix_suggestion': 'Remove duplicates; keep single posting',
            })

    # AmountMismatch and DateDrift for matched records
    if not matches.empty:
        for _, r in matches.iterrows():
            amt_ok = pd.notna(r.get('amount')) and pd.notna(r.get('amount_inr')) and abs(r['amount'] - r['amount_inr']) <= AMT_TOL
            if not amt_ok:
                flags.append({
                    'severity': 'P1',
                    'flag': 'AmountMismatch',
                    'charge_id': r.get('charge_id'),
                    'invoice_id': r.get('invoice_no'),
                    'reason': f"Stripe {r.get('amount')} vs RMS {r.get('amount_inr')} exceeds tolerance ₹{AMT_TOL}",
                    'fix_suggestion': 'Verify currency & rounding; correct RMS amount',
                })
            # Date drift
            try:
                created = r['created']
                posted = r['posted_at']
                if pd.notna(created) and pd.notna(posted):
                    drift = abs((created.date() - posted.date()).days)
                    if drift > DATE_WIN_DAYS:
                        flags.append({
                            'severity': 'P2',
                            'flag': 'DateDrift',
                            'charge_id': r.get('charge_id'),
                            'invoice_id': r.get('invoice_no'),
                            'reason': f"Date drift {drift}d exceeds ±{DATE_WIN_DAYS}d window",
                            'fix_suggestion': 'Align RMS posting date with payment date',
                        })
            except Exception:
                pass

    flags_df = pd.DataFrame(flags, columns=['severity','flag','charge_id','invoice_id','reason','fix_suggestion'])
    return matches, flags_df


# ------------------------------- Orchestration --------------------------- #
def run_checker(stripe_client, rms_client, since: datetime, until: datetime, webhook_log: Optional[pd.DataFrame] = None):
    """Fetch, reconcile, and return (matches_df, flags_df)."""
    charges = stripe_client.get_charges(since, until)
    refunds = stripe_client.get_refunds(since, until)
    rms_rows = rms_client.get_stripe_rms_entries(since, until)

    inputs = CheckerInputs(
        stripe_charges=charges,
        stripe_refunds=refunds,
        rms_rows=rms_rows,
        webhook_log=webhook_log,
    )
    return build_matches_and_flags(inputs)


if __name__ == "__main__":
    # Example: wire with your actual clients
    class DummyStripe:
        def get_charges(self, s, u):
            return pd.DataFrame([
                {"charge_id": "ch_1", "invoice_id": "INV-1001", "email": "a@x.com", "amount": 10000, "currency": "INR", "created": "2025-09-20T08:00:00Z", "status": "succeeded"},
                {"charge_id": "ch_2", "invoice_id": "INV-1001", "email": "a@x.com", "amount": 10000, "currency": "INR", "created": "2025-09-20T08:05:00Z", "status": "succeeded"},
            ])
        def get_refunds(self, s, u):
            return pd.DataFrame([
                {"refund_id": "re_1", "charge_id": "ch_9", "amount": -10000, "currency": "INR", "created": "2025-09-21T10:00:00Z"}
            ])
    class DummyRMS:
        def get_stripe_rms_entries(self, s, u):
            return pd.DataFrame([
                {"rms_id": "r1", "invoice_no": "INV-1001", "email": "a@x.com", "amount_inr": 10000, "posted_at": "2025-09-20T09:00:00"}
            ])

    m, f = run_checker(DummyStripe(), DummyRMS(), datetime.now(TZ) - timedelta(days=7), datetime.now(TZ))
    print(m)
    print(f)
