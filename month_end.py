"""
Month-End Reconciliation Layer
Builds an audit-ready reconciliation statement on top of reconcile().
"""

import time
import numpy as np
import pandas as pd

TODAY = pd.Timestamp.today().normalize()


# ---------------------------------------------------------------------------
# Step 1 helpers – flatten each reconcile() output into a common schema
# ---------------------------------------------------------------------------

def _flatten_matched(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """Convert matched / near-matched pair rows into the unified flat format."""
    if df.empty:
        return pd.DataFrame()
    rows = df.copy()
    rows['category']      = category
    rows['date']          = pd.to_datetime(rows['bank_date'], errors='coerce')
    rows['amount']        = pd.to_numeric(rows['bank_amount'], errors='coerce')
    rows['reference']     = rows['bank_reference'].astype(str)
    rows['description']   = rows['bank_description'].astype(str)
    rows['date_diff_days'] = (
        (pd.to_datetime(rows['bank_date'], errors='coerce')
         - pd.to_datetime(rows['ledger_date'], errors='coerce'))
        .abs().dt.days
    )
    return rows


def _detect_special_categories(df: pd.DataFrame, base_category: str) -> pd.DataFrame:
    """
    Within an unmatched DataFrame, promote rows to DUPLICATE or REVERSAL
    before falling back to the given base_category.

    Performance note
    ----------------
    Reversal detection was previously an O(n²) nested Python loop with two
    pandas .loc calls per iteration — catastrophically slow for 1,000+ unmatched
    rows (e.g. 3,000 rows → ~4.5M pandas operations).

    It is now a vectorized self-merge: O(n log n).
    Logic is identical: match pairs where amount_a ≈ –amount_b within 3 days.
    """
    df = df.copy().reset_index(drop=True)
    df['category'] = base_category

    # --- Duplicate detection: same reference more than once (vectorized, unchanged) ---
    ref_counts = df['reference'].value_counts()
    dup_refs   = ref_counts[ref_counts > 1].index
    df.loc[df['reference'].isin(dup_refs), 'category'] = 'DUPLICATE'

    # --- Reversal detection: vectorized self-merge replaces O(n²) loop ---
    # Keep only rows still in the base category, with valid non-zero amounts.
    candidates = df[
        (df['category'] == base_category) &
        df['amount'].notna() &
        (df['amount'] != 0)
    ]

    if len(candidates) >= 2:
        # Build a thin table: positional index (in df), amount rounded to 2dp, date
        amt_r = candidates['amount'].round(2).to_numpy()
        left  = pd.DataFrame({
            'idx_a':  candidates.index.to_numpy(),
            '_key':   amt_r,
            'date_a': candidates['date'].to_numpy(),
        })
        # Right side uses negated amount as the join key so that
        #   left._key == right._key  ⟺  amount_a ≈ –amount_b
        right = pd.DataFrame({
            'idx_b':  candidates.index.to_numpy(),
            '_key':   -amt_r,
            'date_b': candidates['date'].to_numpy(),
        })

        merged = left.merge(right, on='_key')

        # Drop self-pairs and keep only ordered pairs (avoids double-counting)
        merged = merged[merged['idx_a'] < merged['idx_b']]

        if not merged.empty:
            # Apply 3-day date tolerance (same rule as before)
            valid_dates = pd.notna(merged['date_a']) & pd.notna(merged['date_b'])
            merged = merged[valid_dates]

        if not merged.empty:
            date_diff = (
                pd.to_datetime(merged['date_a']) - pd.to_datetime(merged['date_b'])
            ).abs().dt.days
            merged = merged[date_diff <= 3]

        if not merged.empty:
            reversal_idx = set(merged['idx_a']).union(set(merged['idx_b']))
            df.loc[list(reversal_idx), 'category'] = 'REVERSAL'

    return df


def _flatten_unmatched(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Standardise an unmatched_bank / unmatched_ledger frame into unified format."""
    if df.empty:
        return pd.DataFrame()

    tagged = _detect_special_categories(df.copy(), source)

    is_bank   = source == 'BANK_ONLY_ENTRY'
    is_ledger = source == 'LEDGER_ONLY_ENTRY'

    tagged['bank_date']          = tagged['date']        if is_bank   else pd.NaT
    tagged['bank_amount']        = tagged['amount']      if is_bank   else np.nan
    tagged['bank_reference']     = tagged['reference']   if is_bank   else ''
    tagged['bank_description']   = tagged['description'] if is_bank   else ''
    tagged['ledger_date']        = tagged['date']        if is_ledger else pd.NaT
    tagged['ledger_amount']      = tagged['amount']      if is_ledger else np.nan
    tagged['ledger_reference']   = tagged['reference']   if is_ledger else ''
    tagged['ledger_description'] = tagged['description'] if is_ledger else ''
    tagged['confidence']         = np.nan
    tagged['match_type']         = ''
    tagged['date_diff_days']     = np.nan

    return tagged


def _flatten_composite(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten composite match rows into unified format."""
    if df.empty:
        return pd.DataFrame()
    rows = df.copy()
    rows['category']       = 'COMPOSITE'
    rows['date']           = pd.to_datetime(rows['bank_date'], errors='coerce')
    rows['amount']         = pd.to_numeric(rows['bank_amount'], errors='coerce')
    rows['reference']      = rows['bank_reference'].astype(str)
    rows['description']    = rows['bank_description'].astype(str)
    rows['date_diff_days'] = 0
    return rows


# ---------------------------------------------------------------------------
# Step 2 – status assignment
# ---------------------------------------------------------------------------

def _assign_status(row: pd.Series) -> str:
    cat  = row['category']
    desc = str(row.get('description', '')).upper()

    if cat == 'MATCHED':
        return 'CLEARED'

    if cat == 'COMPOSITE':
        return 'COMPOSITE_CLEARED'

    if cat == 'NEAR_MATCH':
        diff = row.get('date_diff_days', 0)
        diff = 0 if pd.isna(diff) else int(diff)
        return 'LIKELY_CLEARED' if diff <= 3 else 'REVIEW'

    if cat == 'BANK_ONLY_ENTRY':
        if any(kw in desc for kw in ('CHARGE', 'FEE', 'GST', 'INTEREST')):
            return 'ADJUSTMENT_REQUIRED'
        return 'UNRECORDED_RECEIPT'

    if cat == 'LEDGER_ONLY_ENTRY':
        return 'PAYMENT_NOT_PROCESSED'

    if cat == 'REVERSAL':
        return 'REVERSAL_ENTRY'

    if cat == 'DUPLICATE':
        return 'ERROR_DUPLICATE'

    return 'REVIEW'


# ---------------------------------------------------------------------------
# Step 3 – aging
# ---------------------------------------------------------------------------

def _aging_bucket(days) -> str:
    if pd.isna(days):
        return 'UNKNOWN'
    days = int(days)
    if days <= 3:
        return 'CURRENT'
    if days <= 10:
        return 'SHORT_DELAY'
    return 'OLD'


# ---------------------------------------------------------------------------
# Step 5 – suggested journal entries
# ---------------------------------------------------------------------------

def _suggested_entry(row: pd.Series) -> str:
    cat    = row['category']
    desc   = str(row.get('description', '')).upper()
    status = row.get('status', '')

    if cat in ('MATCHED', 'COMPOSITE'):
        return ''

    if cat == 'BANK_ONLY_ENTRY':
        if any(kw in desc for kw in ('CHARGE', 'FEE', 'GST')):
            return 'Dr Bank Charges A/c / Cr Bank A/c'
        if 'INTEREST' in desc:
            return 'Dr Bank A/c / Cr Interest Income A/c'
        return 'Record receipt in ledger'

    if cat == 'NEAR_MATCH' and status == 'REVIEW':
        return 'Post difference adjustment'

    if cat == 'LEDGER_ONLY_ENTRY':
        return 'Check payment run / reverse entry'

    if cat == 'REVERSAL':
        return 'Verify reversal and post correcting entry'

    if cat == 'DUPLICATE':
        return 'Investigate and remove duplicate posting'

    return ''


# ---------------------------------------------------------------------------
# Remarks
# ---------------------------------------------------------------------------

def _build_remarks(row: pd.Series) -> str:
    cat = row['category']

    if cat == 'MATCHED':
        return f"Exact match | Ref: {row.get('bank_reference', '')}"

    if cat == 'COMPOSITE':
        return f"Composite match | Type: {row.get('match_type', '')} | " \
               f"Refs: {str(row.get('bank_reference', ''))[:60]}"

    if cat == 'NEAR_MATCH':
        diff = row.get('date_diff_days', 0)
        diff = 0 if pd.isna(diff) else int(diff)
        conf = row.get('confidence', 0) or 0
        return f"Near match | {diff} day(s) date diff | Confidence: {conf:.0%}"

    if cat == 'BANK_ONLY_ENTRY':
        return 'Bank entry with no ledger counterpart'

    if cat == 'LEDGER_ONLY_ENTRY':
        return 'Ledger entry not yet cleared by bank'

    if cat == 'REVERSAL':
        return 'Possible reversal pair – verify before posting'

    if cat == 'DUPLICATE':
        return 'Duplicate reference – investigate posting'

    return ''


# ---------------------------------------------------------------------------
# Step 1 – main view builder
# ---------------------------------------------------------------------------

_FINAL_COLS = [
    'category', 'status',
    'date', 'amount', 'reference', 'description',
    'bank_date', 'bank_amount', 'bank_reference', 'bank_description',
    'ledger_date', 'ledger_amount', 'ledger_reference', 'ledger_description',
    'confidence', 'match_type', 'date_diff_days',
    'aging_days', 'aging_bucket',
    'remarks', 'suggested_entry',
]


def prepare_month_end_view(result: dict) -> pd.DataFrame:
    """
    Combine all reconcile() outputs into one audit-ready DataFrame.
    """
    parts = []

    if not result['matched'].empty:
        parts.append(_flatten_matched(result['matched'], 'MATCHED'))

    if not result['near_matched'].empty:
        parts.append(_flatten_matched(result['near_matched'], 'NEAR_MATCH'))

    # Composite matches (new)
    if not result.get('composite', pd.DataFrame()).empty:
        parts.append(_flatten_composite(result['composite']))

    if not result['unmatched_bank'].empty:
        parts.append(_flatten_unmatched(result['unmatched_bank'], 'BANK_ONLY_ENTRY'))

    if not result['unmatched_ledger'].empty:
        parts.append(_flatten_unmatched(result['unmatched_ledger'], 'LEDGER_ONLY_ENTRY'))

    if not parts:
        return pd.DataFrame(columns=_FINAL_COLS)

    df = pd.concat(parts, ignore_index=True)

    df['status'] = df.apply(_assign_status, axis=1)

    df['aging_days']   = (TODAY - df['date']).dt.days.clip(lower=0).fillna(0).astype(int)
    df['aging_bucket'] = df['aging_days'].apply(_aging_bucket)

    df['remarks']        = df.apply(_build_remarks, axis=1)
    df['suggested_entry'] = df.apply(_suggested_entry, axis=1)

    return df[[c for c in _FINAL_COLS if c in df.columns]]


# ---------------------------------------------------------------------------
# Step 4 – carry-forward
# ---------------------------------------------------------------------------

def _build_carry_forward(recon_table: pd.DataFrame) -> pd.DataFrame:
    """All non-CLEARED / non-COMPOSITE_CLEARED items become next month's opening items."""
    cf = recon_table[~recon_table['status'].isin(['CLEARED', 'COMPOSITE_CLEARED'])].copy()
    cf = cf.reset_index(drop=True)
    cf.insert(0, 'carry_forward_reason', cf['status'])
    return cf


# ---------------------------------------------------------------------------
# Step 6 – audit-ready summary
# ---------------------------------------------------------------------------

def _build_audit_summary(result: dict, recon_table: pd.DataFrame,
                         opening_ledger_balance: float) -> dict:
    raw = result['summary']

    matched_amt      = result['matched']['bank_amount'].sum()      if not result['matched'].empty      else 0.0
    near_matched_amt = result['near_matched']['bank_amount'].sum() if not result['near_matched'].empty else 0.0
    composite_amt    = (pd.to_numeric(result['composite']['bank_amount'], errors='coerce').sum()
                        if not result.get('composite', pd.DataFrame()).empty else 0.0)
    unmatched_bank_amt   = result['unmatched_bank']['amount'].sum()   if not result['unmatched_bank'].empty   else 0.0
    unmatched_ledger_amt = result['unmatched_ledger']['amount'].sum() if not result['unmatched_ledger'].empty else 0.0

    adj_mask   = recon_table['status'] == 'ADJUSTMENT_REQUIRED'
    total_adj  = recon_table.loc[adj_mask, 'amount'].sum()
    status_counts = recon_table['status'].value_counts().to_dict()

    return {
        'opening_balance_ledger':        round(opening_ledger_balance, 2),
        'closing_balance_bank':          round(raw['bank_total'], 2),
        'closing_balance_ledger':        round(raw['ledger_total'], 2),
        'difference_bank_vs_ledger':     round(raw['difference'], 2),
        'total_matched_amount':          round(matched_amt, 2),
        'total_near_matched_amount':     round(near_matched_amt, 2),
        'total_composite_amount':        round(composite_amt, 2),
        'total_unmatched_bank_amount':   round(unmatched_bank_amt, 2),
        'total_unmatched_ledger_amount': round(unmatched_ledger_amt, 2),
        'total_adjustments_required':    round(total_adj, 2),
        'count_bank_transactions':       raw['total_bank'],
        'count_ledger_entries':          raw['total_ledger'],
        'count_exact_matches':           raw['exact_matches'],
        'count_near_matches':            raw['near_matches'],
        'count_composite_matches':       raw.get('composite_matches', 0),
        'count_unmatched_bank':          raw['unmatched_bank'],
        'count_unmatched_ledger':        raw['unmatched_ledger'],
        'match_rate_pct':                raw['match_rate'],
        **{f'status_{k.lower()}': v for k, v in status_counts.items()},
        'recon_date': TODAY.strftime('%Y-%m-%d'),
    }


# ---------------------------------------------------------------------------
# Step 7 – master export function
# ---------------------------------------------------------------------------

def generate_month_end_recon(result: dict,
                              opening_ledger_balance: float = 0.0) -> dict:
    """
    Master month-end reconciliation function.

    Parameters
    ----------
    result                 : dict returned by reconcile()
    opening_ledger_balance : ledger balance at the start of the period

    Returns
    -------
    {
        'recon_table'  : pd.DataFrame
        'carry_forward': pd.DataFrame
        'summary'      : dict
    }
    """
    _t0 = time.perf_counter()

    recon_table   = prepare_month_end_view(result)
    print(f"[LedgerSync] Month-end view build: {time.perf_counter() - _t0:.3f}s  "
          f"rows={len(recon_table)}")

    _t1 = time.perf_counter()
    carry_forward = _build_carry_forward(recon_table)
    summary       = _build_audit_summary(result, recon_table, opening_ledger_balance)
    print(f"[LedgerSync] Carry-forward + audit summary: {time.perf_counter() - _t1:.3f}s")

    return {
        'recon_table':   recon_table,
        'carry_forward': carry_forward,
        'summary':       summary,
    }


# ---------------------------------------------------------------------------
# Excel export helper
# ---------------------------------------------------------------------------

def export_to_excel(month_end_result: dict, filepath: str) -> None:
    """Write the month-end reconciliation to a formatted Excel workbook."""
    recon_table   = month_end_result['recon_table']
    carry_forward = month_end_result['carry_forward']
    summary       = month_end_result['summary']

    summary_df = pd.DataFrame(list(summary.items()), columns=['Metric', 'Value'])

    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        recon_table.to_excel(writer,   sheet_name='Recon Statement', index=False)
        carry_forward.to_excel(writer, sheet_name='Carry Forward',   index=False)
        summary_df.to_excel(writer,    sheet_name='Summary',         index=False)

        for sheet_name, df in [
            ('Recon Statement', recon_table),
            ('Carry Forward',   carry_forward),
            ('Summary',         summary_df),
        ]:
            ws = writer.sheets[sheet_name]
            for col_idx, col in enumerate(df.columns, start=1):
                max_len = max(
                    len(str(col)),
                    df[col].astype(str).str.len().max() if not df.empty else 0
                )
                ws.column_dimensions[
                    ws.cell(row=1, column=col_idx).column_letter
                ].width = min(max_len + 2, 50)
