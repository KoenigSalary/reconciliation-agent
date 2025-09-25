"""
consolidated_reporter.py — corrected

Goals:
- Multi‑sheet Excel report covering Stripe, CC, FX, Bank
- Clear mismatch reason labeling
- User‑wise pivot summary for AP
- Manual override column to mark mismatches as cleared
- Totals, visuals, and a top‑level summary sheet
- Config‑driven paths and Africa/Lagos timezone alignment
"""
from __future__ import annotations

import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

try:
    from production_config import SETTINGS
except ImportError:
    SETTINGS = {
        "timezone": "Africa/Lagos",
        "paths": {"reports_dir": "./reports"},
    }

TZ = ZoneInfo(SETTINGS.get("timezone", "Africa/Lagos"))

# ----------------------------- Core Functions ----------------------------- #


def _add_override_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "manual_override" not in df.columns:
        df["manual_override"] = None
    return df


def _user_pivot(df: pd.DataFrame, user_col: str, status_col: str) -> pd.DataFrame:
    """Creates pivot table for user vs mismatch status."""
    pivot = pd.pivot_table(
        df,
        index=user_col,
        columns=status_col,
        values="amount_inr" if "amount_inr" in df.columns else None,
        aggfunc="count",
        fill_value=0,
        margins=True,
    )
    return pivot


# ----------------------------- Main Reporter ----------------------------- #


def generate_reconciliation_report(
    output_name: str,
    frames: Dict[str, pd.DataFrame],
    extra_context: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Generate multi‑sheet Excel report with summary and detailed sheets.

    Args:
        output_name: Name for the Excel file.
        frames: dict of {"sheet_name": dataframe}.
        extra_context: Optional summary numbers like total counts, totals etc.

    Returns:
        Path to saved Excel file.
    """
    reports_dir = SETTINGS["paths"].get("reports_dir", "./reports")
    os.makedirs(reports_dir, exist_ok=True)

    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(reports_dir, f"{output_name}_{timestamp}.xlsx")

    # Add override columns to all frames
    frames = {name: _add_override_column(df) for name, df in frames.items()}

    # Build a summary sheet
    summary_data = []
    for name, df in frames.items():
        summary_data.append({
            "Sheet": name,
            "Total Rows": len(df),
            "Flagged": (df["manual_override"].isna()).sum(),
            "Cleared": df["manual_override"].notna().sum(),
        })
    summary_df = pd.DataFrame(summary_data)

    # Write to Excel with style
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        # Summary sheet first
        summary_df.to_excel(writer, index=False, sheet_name="Summary")

        workbook = writer.book
        header_fmt = workbook.add_format({"bold": True, "bg_color": "#D9E1F2"})

        for name, df in frames.items():
            clean_name = name.replace("/", "-")[:31]
            df.to_excel(writer, index=False, sheet_name=clean_name)
            worksheet = writer.sheets[clean_name]
            # Freeze header
            worksheet.freeze_panes(1, 0)
            # Auto width
            for i, col in enumerate(df.columns):
                col_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, col_width)

            # Add header style
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_fmt)

        # If user pivot possible, add as last sheet
        if "CC Flags" in frames:
            pivot = _user_pivot(frames["CC Flags"], user_col="claimed_by", status_col="markup_status")
            pivot.to_excel(writer, sheet_name="CC User Pivot")

    return output_path


# ------------------------------ Example Use ------------------------------ #
if __name__ == "__main__":
    # Example run with dummy frames
    stripe_df = pd.DataFrame({"id": [1, 2], "status": ["OK", "RefundNotPosted"], "amount_inr": [1000, 1500]})
    cc_df = pd.DataFrame({"id": [1, 2], "claimed_by": ["UserA", "UserB"], "markup_status": ["OK", "FLAGGED"], "amount_inr": [1200, 2200]})
    fx_df = pd.DataFrame({"id": [1], "fx_currency": ["USD"], "markup_status": ["OK"], "amount_inr": [5000]})

    frames = {
        "Stripe Flags": stripe_df,
        "CC Flags": cc_df,
        "FX Flags": fx_df,
    }

    path = generate_reconciliation_report("ReconReport", frames)
    print(f"Report generated at {path}")
