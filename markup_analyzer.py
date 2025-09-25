"""
Corrected markup_analyzer.py

Integrates tightly with fx_detector to:
- Use detected FX context for DCC bypass and accurate foreign vs INR handling
- Calculate markup percentage and INR delta vs interbank rate
- Load thresholds dynamically from production_config.py
- Output structured result with status and reason codes
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

try:
    from production_config import SETTINGS
except ImportError:
    SETTINGS = {
        "thresholds": {
            "fx_markup_tolerance_pct": 2.5,
            "fx_inr_tolerance": 100,
        }
    }

FX_MARKUP_TOL = SETTINGS["thresholds"].get("fx_markup_tolerance_pct", 2.5)
FX_INR_TOL = SETTINGS["thresholds"].get("fx_inr_tolerance", 100)


@dataclass
class MarkupResult:
    is_flagged: bool
    markup_pct: Optional[float]
    inr_diff: Optional[float]
    status: str
    reason: str


def analyze_markup(
    foreign_amount: Optional[float],
    charged_inr: Optional[float],
    interbank_rate: Optional[float],
    fx_context: Optional[Dict[str, Any]] = None,
) -> MarkupResult:
    """
    Perform markup and INR deviation analysis.

    Args:
        foreign_amount: Foreign currency amount of the transaction.
        charged_inr: Actual INR charged on statement.
        interbank_rate: IBR for txn date.
        fx_context: Context from fx_detector.detect_fx_context()

    Returns:
        MarkupResult
    """
    if not foreign_amount or not charged_inr or not interbank_rate:
        return MarkupResult(
            is_flagged=False,
            markup_pct=None,
            inr_diff=None,
            status="SKIPPED",
            reason="Missing core values for analysis",
        )

    # Skip markup analysis for DCC cases
    if fx_context and fx_context.get("is_dcc"):
        return MarkupResult(
            is_flagged=False,
            markup_pct=None,
            inr_diff=None,
            status="BYPASS",
            reason="DCC transaction detected, markup check not applicable",
        )

    expected_inr = foreign_amount * interbank_rate
    inr_diff = round(charged_inr - expected_inr, 2)
    actual_rate = charged_inr / foreign_amount
    markup_pct = round(((actual_rate - interbank_rate) / interbank_rate) * 100, 2)

    # Check thresholds
    is_markup_exceeded = markup_pct > FX_MARKUP_TOL
    is_inr_diff_exceeded = abs(inr_diff) > FX_INR_TOL

    if is_markup_exceeded or is_inr_diff_exceeded:
        status = "FLAGGED"
        reasons = []
        if is_markup_exceeded:
            reasons.append(f"Markup {markup_pct}% exceeds {FX_MARKUP_TOL}%")
        if is_inr_diff_exceeded:
            reasons.append(f"INR difference ₹{abs(inr_diff)} exceeds ₹{FX_INR_TOL}")
        reason = "; ".join(reasons)
        return MarkupResult(True, markup_pct, inr_diff, status, reason)

    return MarkupResult(
        is_flagged=False,
        markup_pct=markup_pct,
        inr_diff=inr_diff,
        status="OK",
        reason="Within thresholds",
    )


# Convenience wrapper for batch processing

def batch_analyze_markup(df, cols: Dict[str, str]) -> Any:
    """
    Annotate dataframe with markup analysis results.

    cols mapping must include:
        - foreign_amount
        - charged_inr
        - interbank_rate
        - fx_is_dcc (optional)
    """
    out = df.copy()
    out["markup_pct"] = None
    out["inr_diff"] = None
    out["markup_status"] = None
    out["markup_reason"] = None

    for idx, row in out.iterrows():
        fx_context = {"is_dcc": row.get(cols.get("fx_is_dcc"))} if cols.get("fx_is_dcc") else {}
        result = analyze_markup(
            foreign_amount=row.get(cols["foreign_amount"]),
            charged_inr=row.get(cols["charged_inr"]),
            interbank_rate=row.get(cols["interbank_rate"]),
            fx_context=fx_context,
        )
        out.at[idx, "markup_pct"] = result.markup_pct
        out.at[idx, "inr_diff"] = result.inr_diff
        out.at[idx, "markup_status"] = result.status
        out.at[idx, "markup_reason"] = result.reason

    return out
